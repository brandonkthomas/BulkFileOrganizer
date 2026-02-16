<div align="center">
  <h3 align="center">BulkFileOrganizer</h3>

  <p align="center">
    High-performance CLI to bulk copy/move files into organized buckets
    <br />
    <br />
    <a href="https://dotnet.microsoft.com/en-us/languages/csharp"><img alt="C#" src="https://custom-icon-badges.demolab.com/badge/C%23-%23239120.svg?logo=cshrp&logoColor=white"></a>
  </p>
</div>

## About the Project

BulkFileOrganizer is a high-performance .NET CLI that bulk copies or moves files into evenly distributed, organized bucket directories for easier storage and management.
Can be used when managing extremely large numbers of files; the CLI will redistribute them into buckets to reduce filesystem slowdowns.

## Usage

```text
Usage:
  BulkFileOrganizer --ext <extension> --input <dir> --output <dir> [--action copy|move] [--depth N] [--workers N] [--queue N] [--progress N] [--test N]

Required:
  --ext      File extension to include (e.g. .jpg or jpg). Case-insensitive.
  --input    Root directory to search recursively.
  --output   Output root directory. Evenly sized bucket directories are created under this.

Optional:
  --action    copy (default) or move
  --depth     Max recursion depth. -1 (default) = infinite. 0 = only input directory.
  --workers   Number of parallel workers. Default = CPU/2
  --queue     Bounded queue capacity. Default = 10000
  --progress  Print progress every N processed/enumerated items. Default = 10000
  --test      Generate N empty files in the input directory (with the chosen extension) before processing.

Notes:
  - Files are evenly distributed into hex-named bucket directories (0000, 0001, ...) based on the total file count.
  - Name collisions are resolved by appending a short hash suffix.
```
