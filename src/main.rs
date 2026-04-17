use anyhow::{anyhow, bail, ensure, Result};
use log::{error, trace, warn};
use serde::{Deserialize, Deserializer, Serialize};
use simplelog::{Config, TermLogger, TerminalMode};
use std::{
    fs::File,
    io::BufReader,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time,
};
use std::collections::{BTreeMap, HashMap};
use structopt::StructOpt;
use structopt_flags::LogLevel;
use tempfile::TempDir;
use uasset::{AssetHeader, ObjectExport, ObjectReference};
use walkdir::WalkDir;
use uasset::enums::ObjectFlags;

mod fproperty;

const UASSET_EXTENSIONS: [&str; 2] = ["uasset", "umap"];

fn is_uasset<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();
    if let Some(extension) = path.extension() {
        let extension = extension.to_string_lossy();
        UASSET_EXTENSIONS.contains(&extension.as_ref())
    } else {
        false
    }
}

#[derive(Debug, PartialEq)]
enum Validation {
    #[allow(dead_code)]
    AssetReferencesExist,
    HasEngineVersion,
}

#[derive(Debug)]
enum ValidationMode {
    All,
    Individual(Vec<Validation>),
}

impl ValidationMode {
    pub fn includes(&self, validation: &Validation) -> bool {
        if let Self::Individual(modes) = self {
            modes.contains(validation)
        } else {
            true
        }
    }
}

fn parse_validation_mode(src: &str) -> Result<ValidationMode> {
    if src == "All" {
        Ok(ValidationMode::All)
    } else {
        let src = src.to_string();
        let modes = src.split(',');
        let mut parsed_modes = Vec::new();
        for mode in modes {
            #[allow(clippy::unimplemented)]
            let parsed_mode = match mode {
                "AssetReferencesExist" => unimplemented!("Validation::AssetReferencesExist"),
                "HasEngineVersion" => Validation::HasEngineVersion,
                _ => bail!("Unrecognized validation mode {}", mode),
            };
            parsed_modes.push(parsed_mode);
        }
        Ok(ValidationMode::Individual(parsed_modes))
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "uasset",
    about = "Parse and display info about files in the Unreal Engine uasset format"
)]
struct CommandOptions {
    #[structopt(flatten)]
    verbose: structopt_flags::QuietVerbose,
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Generating timings for loading all the given assets
    Benchmark {
        /// Assets to load, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
    },
    /// Show all the fields of the `AssetHeader` for the listed assets
    Dump {
        /// Assets to dump, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
    },
    /// Run asset validations on the listed assets
    Validate {
        /// Assets to validate, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
        /// Perforce changelist to examine files from
        #[structopt(long)]
        perforce_changelist: Option<NonZeroU32>,
        /// Validation mode, [All|Mode1,Mode2,..],
        ///
        /// Valid modes are:
        ///  - `AssetReferencesExist`: Verify that all asset references to or from the listed assets are valid
        ///  - `HasEngineVersion`: Verify that every asset has a valid engine version
        #[structopt(long, parse(try_from_str = parse_validation_mode), verbatim_doc_comment)]
        mode: Option<ValidationMode>,
    },
    /// Show the imports for the listed assets
    ListImports {
        /// Assets to list imports for, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
        /// Skip showing imports for code references (imports that start with /Script/)
        #[structopt(long)]
        skip_code_imports: bool,
    },
    /// Show the object types of the public exports of the listed assets
    ListObjectTypes {
        /// Assets to list object types for, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
    },
    /// Dump some information about the thumbnails for the listed assets
    DumpThumbnailInfo {
        /// Assets to dump thumbnail info for, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
    },
    /// List component types and tags from Blueprint assets
    ListBlueprintComponents {
        /// Blueprint assets to inspect, directories will be recursively searched for assets
        assets_or_directories: Vec<PathBuf>,
    },
}

#[derive(Serialize)]
struct ComponentInfo {
    class: String,
    tags: Vec<String>,
}

#[derive(Serialize)]
struct BlueprintInfo {
    components: Vec<ComponentInfo>,
    parent: Option<String>,
    /// Populated when the asset could not be parsed, so downstream consumers
    /// can distinguish "no components" from "parse failed".
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn recursively_walk_uassets(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .flat_map(|path| {
            if path.is_dir() {
                WalkDir::new(path)
                    .follow_links(true)
                    .into_iter()
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| !name.starts_with('.') && is_uasset(name))
                    })
                    .filter(|entry| entry.file_type().is_file())
                    .map(|entry| entry.path().to_path_buf())
                    .collect()
            } else {
                vec![path]
            }
        })
        .collect()
}

#[derive(Debug)]
enum PerforceAction {
    Add,
    Edit,
    Delete,
    Branch,
    MoveAdd,
    MoveDelete,
    Integrate,
    Import,
    Purge,
    Archive,
}

impl<'de> Deserialize<'de> for PerforceAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_ref() {
            "add" => Self::Add,
            "edit" => Self::Edit,
            "delete" => Self::Delete,
            "branch" => Self::Branch,
            "move/add" => Self::MoveAdd,
            "move/delete" => Self::MoveDelete,
            "integrate" => Self::Integrate,
            "import" => Self::Import,
            "purge" => Self::Purge,
            "archive" => Self::Archive,
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "Invalid PerforceAction '{}'",
                    s
                )))
            }
        })
    }
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct PerforceFilesRecord {
    pub action: PerforceAction,
    pub change: String,
    pub depot_file: String,
    pub rev: String,
    pub time: String,
    #[serde(rename = "type")]
    pub file_type: String,
}

fn fetch_perforce_uassets(changelist: NonZeroU32) -> Result<(Option<TempDir>, Vec<PathBuf>)> {
    let asset_dir = TempDir::new()?;
    let mut asset_paths = Vec::new();

    let command = std::process::Command::new("p4")
        .args(["-z", "tag", "-Mj"])
        .arg("files")
        .arg(format!("@={}", changelist))
        .output()?;

    let stdout = std::str::from_utf8(&command.stdout)?;
    if !command.status.success() {
        let stderr = std::str::from_utf8(&command.stderr)?;
        bail!(
            "Failed to run `p4 files`:\nstdout: {}\nstderr: {}",
            stdout,
            stderr
        );
    }

    for line in stdout.lines() {
        let record: PerforceFilesRecord = serde_json::from_str(line)?;
        let modified_file = match record.action {
            PerforceAction::Delete | PerforceAction::MoveDelete | PerforceAction::Purge | PerforceAction::Archive => None,
            _ => Some(&record.depot_file),
        };

        if let Some(path) = modified_file {
            if !is_uasset(path) {
                trace!("ignoring modified file {}, not an uasset", path);
                continue;
            }

            trace!("downloading file {}", record.depot_file);

            let path = PathBuf::from(&path[2..]);
            let local_path = asset_dir.path().join(path);
            if let Some(parent_path) = local_path.parent() {
                std::fs::create_dir_all(parent_path)?;
            }

            let file = File::create(&local_path)?;
            let filespec = format!("{}@={}", record.depot_file, changelist);
            let print_command = std::process::Command::new("p4")
                .arg("print")
                .arg("-q")
                .arg(&filespec)
                .stdout(std::process::Stdio::from(file))
                .output()?;

            ensure!(
                print_command.status.success(),
                "Failed to run `p4 print {}`",
                filespec
            );

            asset_paths.push(local_path);
        } else {
            trace!(
                "ignoring file {} with non-modification action {:?}",
                record.depot_file,
                record.action
            );
        }
    }

    if asset_paths.is_empty() {
        Ok((None, asset_paths))
    } else {
        Ok((Some(asset_dir), asset_paths))
    }
}

fn try_parse(asset_path: &Path) -> Result<AssetHeader<BufReader<File>>> {
    trace!("reading {}", asset_path.display());
    match File::open(asset_path) {
        Ok(file) => match AssetHeader::new(BufReader::new(file)) {
            Ok(header) => Ok(header),
            Err(error) => Err(anyhow!(
                "failed to parse {}: {:?}",
                asset_path.display(),
                error
            )),
        },
        Err(error) => Err(anyhow!(
            "failed to load {}: {:?}",
            asset_path.display(),
            error
        )),
    }
}

fn try_parse_or_log<T: FnOnce(AssetHeader<BufReader<File>>)>(
    asset_path: &Path,
    callback: T,
) -> bool {
    trace!("reading {}", asset_path.display());
    match File::open(asset_path) {
        Ok(file) => match AssetHeader::new(BufReader::new(file)) {
            Ok(header) => {
                callback(header);
                true
            }
            Err(error) => {
                error!("failed to parse {}: {:?}", asset_path.display(), error);
                false
            }
        },
        Err(error) => {
            error!("failed to load {}: {:?}", asset_path.display(), error);
            false
        }
    }
}

fn main() -> Result<()> {
    let options = CommandOptions::from_args();
    TermLogger::init(
        options.verbose.get_level_filter(),
        Config::default(),
        TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    match options.cmd {
        Command::Benchmark {
            assets_or_directories,
        } => {
            let start = time::Instant::now();
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            println!("Scanning directories took {:?}", start.elapsed());

            let load_start = time::Instant::now();
            let num_assets = asset_paths.len();
            let (num_errs, num_imports) = asset_paths
                .into_iter()
                .map(|asset_path| {
                    let mut num_imports = 0;
                    let reader = |header: AssetHeader<_>| {
                        trace!("found {} imports", header.imports.len());
                        num_imports = header.imports.len();
                    };

                    if try_parse_or_log(&asset_path, reader) {
                        (0, num_imports)
                    } else {
                        (1, 0)
                    }
                })
                .fold((0, 0), |(sum_errs, sum_imports), (errs, imports)| {
                    (sum_errs + errs, sum_imports + imports)
                });
            let load_duration = load_start.elapsed();

            println!(
                "Loading {} assets ({} failed) with {} imports took {:?}",
                num_assets, num_errs, num_imports, load_duration,
            );
            println!("Total execution took {:?}", start.elapsed());
        }
        Command::Dump {
            assets_or_directories,
        } => {
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            for asset_path in asset_paths {
                try_parse_or_log(&asset_path, |header| {
                    println!("{}:", asset_path.display());
                    println!("{:#?}", header);
                    println!();
                });
            }
        }
        Command::Validate {
            assets_or_directories,
            mode,
            perforce_changelist,
        } => {
            let mode = mode.unwrap_or(ValidationMode::All);
            let mut errors = Vec::new();
            let (temp_dir, asset_paths) = {
                let mut asset_paths = recursively_walk_uassets(assets_or_directories);
                if let Some(changelist) = perforce_changelist {
                    let (asset_dir, mut assets) = fetch_perforce_uassets(changelist)?;
                    asset_paths.append(&mut assets);
                    (asset_dir, asset_paths)
                } else {
                    (None, asset_paths)
                }
            };

            let mut num_evaluated_assets = 0;
            for asset_path in asset_paths {
                num_evaluated_assets += 1;
                match try_parse(&asset_path) {
                    Ok(header) => {
                        if header.engine_version.is_empty()
                            && mode.includes(&Validation::HasEngineVersion)
                        {
                            errors.push(format!(
                                "{}: Missing engine version, resave with a versioned editor",
                                asset_path.display()
                            ));
                        }
                    }
                    Err(error) => {
                        errors.push(format!(
                            "{}: Could not parse asset: {}",
                            asset_path.display(),
                            error
                        ));
                    }
                };
            }

            if let Some(temp_dir) = temp_dir {
                temp_dir.close()?;
            }

            if !errors.is_empty() {
                eprintln!(
                    "Encountered {} errors in {} assets:",
                    errors.len(),
                    num_evaluated_assets
                );
                for error in errors {
                    eprintln!("{}", error);
                }
                bail!("Validation failed");
            } else {
                println!("Checked {} assets, no errors", num_evaluated_assets);
            }
        }
        Command::ListImports {
            assets_or_directories,
            skip_code_imports,
        } => {
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            for asset_path in asset_paths {
                try_parse_or_log(&asset_path, |header| {
                    println!("{}:", asset_path.display());
                    for import in header.package_import_iter() {
                        if !skip_code_imports || !import.starts_with("/Script/") {
                            println!("  {}", import);
                        }
                    }
                });
            }
        }
        Command::ListObjectTypes {
            assets_or_directories,
        } => {
            let checked_flags = ObjectFlags::Standalone as u32 | ObjectFlags::Public as u32 | ObjectFlags::Transient as u32 | ObjectFlags::ClassDefaultObject as u32;
            let expected_flags = ObjectFlags::Standalone as u32 | ObjectFlags::Public as u32;
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            let mut asset_types = HashMap::new();
            for asset_path in asset_paths {
                try_parse_or_log(&asset_path, |header| {
                    let expected_object_name_start_index = header.package_name.rfind('/').map(|i| i + 1).unwrap_or_default();
                    let expected_object_name = header.package_name[expected_object_name_start_index..].to_string();
                    let expected_object_name_index = header.find_name(&expected_object_name);

                    let asset_object = header.exports.iter().find(|export| Some(export.object_name) == expected_object_name_index && export.is_asset && export.object_flags & checked_flags == expected_flags);
                    let asset_type = asset_object.and_then(|asset_object| {
                        let class_name = match asset_object.class() {
                            ObjectReference::Export { export_index } => header.exports.get(export_index).map(|e| e.object_name),
                            ObjectReference::Import { import_index } => header.imports.get(import_index).map(|e| e.object_name),
                            ObjectReference::None => None,
                        };

                        class_name.and_then(|name| header.resolve_name(&name).map(|s| s.to_string()).ok())
                    });

                    asset_types.insert(asset_path.display().to_string(), asset_type);
                });
            }
            println!("{json}", json = serde_json::to_string(&asset_types)?);
        }
        Command::DumpThumbnailInfo {
            assets_or_directories,
        } => {
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            for asset_path in asset_paths {
                try_parse_or_log(&asset_path, |mut header| {
                    println!("{}:", asset_path.display());
                    match header.thumbnail_iter() {
                        Ok(thumbnail_iter) => {
                            for thumbnail_info in thumbnail_iter {
                                match thumbnail_info {
                                    Ok(thumbnail_info) => println!("{:#?}", thumbnail_info),
                                    Err(error) => error!(
                                        "failed to read a specific thumbnail for {}: {:?}",
                                        asset_path.display(),
                                        error
                                    ),
                                }
                            }
                        }
                        Err(error) => {
                            error!(
                                "failed to read thumbnails for {}: {:?}",
                                asset_path.display(),
                                error
                            );
                        }
                    }
                    println!();
                });
            }
        }
        Command::ListBlueprintComponents {
            assets_or_directories,
        } => {
            let asset_paths = recursively_walk_uassets(assets_or_directories);
            // BTreeMap so JSON output has deterministic key order — otherwise
            // diffing two runs (e.g. across commits) is noise.
            let mut results: BTreeMap<String, BlueprintInfo> = BTreeMap::new();
            for asset_path in asset_paths {
                let key = asset_path.display().to_string();
                let info = match try_parse(&asset_path) {
                    Ok(mut header) => extract_blueprint_info(&mut header),
                    Err(error) => {
                        error!("{}", error);
                        BlueprintInfo {
                            components: vec![],
                            parent: None,
                            error: Some(format!("{}", error)),
                        }
                    }
                };
                results.insert(key, info);
            }
            println!("{json}", json = serde_json::to_string(&results)?);
        }
    }

    Ok(())
}

/// Maximum depth when walking an import's outer chain or an export's
/// `Default__` class chain. Bounds protect against corrupt or cyclic asset
/// metadata that would otherwise cause an infinite loop.
const MAX_CHAIN_DEPTH: usize = 16;

/// Resolve an import's name lazily. Avoids the eager
/// `Vec<String>` allocation of every import name in the file — callers
/// typically look up a handful of positions, not all of them.
fn import_name<R>(header: &AssetHeader<R>, idx: usize) -> Option<String> {
    let import = header.imports.get(idx)?;
    header.resolve_name(&import.object_name).ok().map(|s| s.to_string())
}

/// Get the package path for an import by walking up its outer chain to the root package.
fn resolve_import_package<R>(header: &AssetHeader<R>, import_index: usize) -> Option<String> {
    let mut current = import_index;
    for _ in 0..MAX_CHAIN_DEPTH {
        let import = header.imports.get(current)?;
        match import.outer() {
            ObjectReference::Import { import_index: outer } => {
                current = outer;
            }
            _ => {
                return header.resolve_name(&import.object_name).ok().map(|s| s.to_string());
            }
        }
    }
    warn!("resolve_import_package: exceeded max chain depth starting at import {}", import_index);
    None
}

/// Resolve the class name of an export, following the `Default__` indirection
/// used by inherited component overrides. Walks up to `MAX_CHAIN_DEPTH` hops
/// for deeply nested BP inheritance; without a bound a cyclic asset would
/// loop forever, and without a loop a two-deep inheritance chain would be
/// silently misresolved (components dropped).
fn resolve_export_class_name<R>(
    header: &AssetHeader<R>,
    export: &ObjectExport,
) -> Option<String> {
    let mut current_class = export.class();
    let mut last_name: Option<String> = None;
    for _ in 0..MAX_CHAIN_DEPTH {
        match current_class {
            ObjectReference::Import { import_index } => {
                return import_name(header, import_index);
            }
            ObjectReference::Export { export_index } => {
                let inner = header.exports.get(export_index)?;
                let resolved = header
                    .resolve_name(&inner.object_name)
                    .ok()
                    .map(|s| s.to_string());
                match resolved {
                    Some(name) if name.starts_with("Default__") => {
                        last_name = Some(name);
                        current_class = inner.class();
                    }
                    Some(name) => return Some(name),
                    None => return last_name,
                }
            }
            ObjectReference::None => return last_name,
        }
    }
    last_name
}

/// Extract component info and parent Blueprint from a parsed asset header.
fn extract_blueprint_info<R: std::io::Read + std::io::Seek>(
    header: &mut AssetHeader<R>,
) -> BlueprintInfo {
    // Resolve every export's class name once and reuse across the main,
    // canary, and trace passes below. Resolution walks imports + exports,
    // so doing it per-pass was ~3x redundant work on large assets.
    let class_names: Vec<Option<String>> = header
        .exports
        .iter()
        .map(|export| resolve_export_class_name(header, export))
        .collect();

    let mut components = Vec::new();

    // If the file's name table doesn't contain `ComponentTags` / `ArrayProperty`
    // at all, no export can possibly carry the property we extract. Skip the
    // whole per-export scan.
    if let Some(indices) = fproperty::NameIndices::lookup(&header.names) {
        // Canary enabled only when the user has raised verbosity (-v). Scanning
        // every rejected export's serial bytes costs real I/O, and the sweep we
        // ran over Content/Entities + Content/Blueprints produced zero hits, so
        // running it by default would pay that cost for no benefit. With -v it
        // stays available as a regression check when content changes.
        let canary_enabled = log::log_enabled!(log::Level::Info);

        let component_exports: Vec<(usize, String)> = class_names
            .iter()
            .enumerate()
            .filter_map(|(idx, class)| {
                let name = class.as_ref()?;
                name.ends_with("Component").then(|| (idx, name.clone()))
            })
            .collect();

        for (export_idx, class_name) in component_exports {
            let export = &header.exports[export_idx];
            let serial_offset = export.serial_offset as u64;
            let serial_size = export.serial_size as u64;
            let tags = fproperty::extract_component_tags(
                &mut header.archive,
                &indices,
                &header.names,
                serial_offset,
                serial_size,
            );
            if !tags.is_empty() {
                components.push(ComponentInfo { class: class_name, tags });
            }
        }

        if canary_enabled {
            let rejected: Vec<(usize, String, String)> = class_names
                .iter()
                .enumerate()
                .filter_map(|(idx, class)| {
                    let class = class.as_ref()?;
                    if class.ends_with("Component") {
                        return None;
                    }
                    let export = header.exports.get(idx)?;
                    let name = header
                        .resolve_name(&export.object_name)
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    Some((idx, name, class.clone()))
                })
                .collect();

            for (export_idx, export_name, class) in rejected {
                let export = &header.exports[export_idx];
                if fproperty::serial_contains_component_tags_name(
                    &mut header.archive,
                    indices.component_tags,
                    export.serial_offset as u64,
                    export.serial_size as u64,
                ) {
                    warn!(
                        "export[{}] name={:?} class={:?} rejected by class filter but serial contains ComponentTags; filter may miss a component",
                        export_idx, export_name, class
                    );
                }
            }
        }
    }

    if log::log_enabled!(log::Level::Trace) {
        for (export_idx, export) in header.exports.iter().enumerate() {
            let class = class_names[export_idx].as_deref().unwrap_or("(none)");
            let export_name = header
                .resolve_name(&export.object_name)
                .map(|s| s.to_string())
                .unwrap_or_default();
            if class.contains("Component")
                || class.contains("Mesh")
                || export_name.contains("Mesh")
                || export_name.contains("Component")
            {
                trace!(
                    "export[{}]: name={:?} class={:?} offset={} size={}",
                    export_idx, export_name, class, export.serial_offset, export.serial_size
                );
            }
        }
    }

    let parent = find_parent_blueprint(header);
    BlueprintInfo { components, parent, error: None }
}

/// Find the parent Blueprint package path by tracing the generated class's `super_index`.
fn find_parent_blueprint<R>(header: &AssetHeader<R>) -> Option<String> {
    let bgc_import_idx = header.imports.iter().position(|imp| {
        matches!(
            header.resolve_name(&imp.object_name).ok().as_deref(),
            Some("BlueprintGeneratedClass")
        )
    })?;

    let gen_class_export = header.exports.iter().find(|export| {
        matches!(
            export.class(),
            ObjectReference::Import { import_index } if import_index == bgc_import_idx
        )
    })?;

    match gen_class_export.superclass() {
        ObjectReference::Import { import_index } => {
            let package_path = resolve_import_package(header, import_index)?;
            // Only return Blueprint parents (from /Game/), not C++ parents (from /Script/).
            if package_path.starts_with("/Game/") {
                Some(package_path)
            } else {
                None
            }
        }
        _ => None,
    }
}
