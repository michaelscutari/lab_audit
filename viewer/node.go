package main

import (
	"path/filepath"
	"strings"
)

// Node represents a file or directory in the tree.
type Node struct {
	Name      string
	FullPath  string
	Size      int64  // Logical size (asize)
	Usage     int64  // Disk usage (dsize)
	IsDir     bool
	FileType  string
	Extension string
	ItemCount int64 // For dirs: total files underneath
	Depth     int
}

// FileTypeStat holds aggregated stats for a file type category.
type FileTypeStat struct {
	Type  string
	Size  int64
	Count int
}

// getFileType categorizes a file by its extension.
func getFileType(name string) string {
	ext := strings.ToLower(filepath.Ext(name))

	codeExts := map[string]bool{
		".go": true, ".py": true, ".js": true, ".ts": true, ".jsx": true, ".tsx": true,
		".c": true, ".cpp": true, ".h": true, ".hpp": true, ".rs": true, ".java": true,
		".rb": true, ".php": true, ".swift": true, ".kt": true, ".scala": true,
		".sh": true, ".bash": true, ".zsh": true, ".fish": true, ".ps1": true,
		".sql": true, ".r": true, ".m": true, ".f90": true, ".jl": true,
	}

	dataExts := map[string]bool{
		".csv": true, ".json": true, ".xml": true, ".yaml": true, ".yml": true,
		".parquet": true, ".avro": true, ".orc": true, ".hdf5": true, ".h5": true,
		".npy": true, ".npz": true, ".pkl": true, ".pickle": true, ".feather": true,
		".db": true, ".sqlite": true, ".sqlite3": true, ".mdb": true,
	}

	mediaExts := map[string]bool{
		".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".bmp": true,
		".svg": true, ".webp": true, ".ico": true, ".tiff": true, ".psd": true,
		".mp4": true, ".avi": true, ".mov": true, ".mkv": true, ".wmv": true,
		".mp3": true, ".wav": true, ".flac": true, ".aac": true, ".ogg": true,
		".webm": true, ".m4v": true, ".m4a": true,
	}

	archiveExts := map[string]bool{
		".zip": true, ".tar": true, ".gz": true, ".bz2": true, ".xz": true,
		".7z": true, ".rar": true, ".tgz": true, ".tbz2": true, ".lz4": true,
		".zst": true, ".iso": true, ".dmg": true,
	}

	docExts := map[string]bool{
		".pdf": true, ".doc": true, ".docx": true, ".xls": true, ".xlsx": true,
		".ppt": true, ".pptx": true, ".odt": true, ".ods": true, ".odp": true,
		".txt": true, ".md": true, ".rst": true, ".tex": true, ".rtf": true,
		".epub": true, ".mobi": true,
	}

	configExts := map[string]bool{
		".toml": true, ".ini": true, ".cfg": true, ".conf": true, ".env": true,
		".gitignore": true, ".dockerignore": true, ".editorconfig": true,
		".htaccess": true, ".properties": true,
	}

	switch {
	case codeExts[ext]:
		return "code"
	case dataExts[ext]:
		return "data"
	case mediaExts[ext]:
		return "media"
	case archiveExts[ext]:
		return "archive"
	case docExts[ext]:
		return "doc"
	case configExts[ext]:
		return "config"
	default:
		return "other"
	}
}

// getExtension returns the lowercase extension of a filename.
func getExtension(name string) string {
	ext := filepath.Ext(name)
	if ext == "" {
		return "(none)"
	}
	return strings.ToLower(ext)
}
