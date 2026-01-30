package main

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// DB wraps the DuckDB connection and provides query methods.
type DB struct {
	conn        *sql.DB
	parquetPath string
}

// GlobalStats holds aggregate statistics for the entire dataset.
type GlobalStats struct {
	TotalItems int64
	TotalFiles int64
	TotalDirs  int64
	TotalUsage int64
	TotalSize  int64
}

// OpenDB opens a DuckDB connection for querying the parquet file.
func OpenDB(parquetPath string) (*DB, error) {
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Verify the parquet file is readable
	var count int64
	err = conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM '%s'", parquetPath)).Scan(&count)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read parquet file: %w", err)
	}

	return &DB{
		conn:        conn,
		parquetPath: parquetPath,
	}, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// GetGlobalStats returns aggregate statistics for the entire dataset.
func (db *DB) GetGlobalStats() (*GlobalStats, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_items,
			SUM(CASE WHEN NOT is_dir THEN 1 ELSE 0 END) as total_files,
			SUM(CASE WHEN is_dir THEN 1 ELSE 0 END) as total_dirs,
			SUM(CASE WHEN NOT is_dir THEN usage ELSE 0 END) as total_usage,
			SUM(CASE WHEN NOT is_dir THEN size ELSE 0 END) as total_size
		FROM '%s'
	`, db.parquetPath)

	var stats GlobalStats
	err := db.conn.QueryRow(query).Scan(
		&stats.TotalItems,
		&stats.TotalFiles,
		&stats.TotalDirs,
		&stats.TotalUsage,
		&stats.TotalSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get global stats: %w", err)
	}

	return &stats, nil
}

// GetRootPath returns the root path from the parquet file.
func (db *DB) GetRootPath() (string, error) {
	query := fmt.Sprintf(`
		SELECT path FROM '%s'
		WHERE parent = '' OR parent IS NULL
		LIMIT 1
	`, db.parquetPath)

	var rootPath string
	err := db.conn.QueryRow(query).Scan(&rootPath)
	if err != nil {
		return "", fmt.Errorf("failed to get root path: %w", err)
	}

	return rootPath, nil
}

// GetNode returns the node at the given path.
func (db *DB) GetNode(path string) (*Node, error) {
	query := fmt.Sprintf(`
		SELECT path, name, size, usage, is_dir, item_count, depth
		FROM '%s'
		WHERE path = ?
		LIMIT 1
	`, db.parquetPath)

	var node Node
	var itemCount int64
	err := db.conn.QueryRow(query, path).Scan(
		&node.FullPath,
		&node.Name,
		&node.Size,
		&node.Usage,
		&node.IsDir,
		&itemCount,
		&node.Depth,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	node.ItemCount = itemCount
	if !node.IsDir {
		node.FileType = getFileType(node.Name)
		node.Extension = getExtension(node.Name)
	} else {
		node.FileType = "dir"
	}

	return &node, nil
}

// GetChildren returns the direct children of a directory, ordered by usage descending.
func (db *DB) GetChildren(parentPath string) ([]*Node, error) {
	query := fmt.Sprintf(`
		SELECT path, name, size, usage, is_dir, item_count, depth
		FROM '%s'
		WHERE parent = ?
		ORDER BY usage DESC
	`, db.parquetPath)

	rows, err := db.conn.Query(query, parentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to query children: %w", err)
	}
	defer rows.Close()

	var children []*Node
	for rows.Next() {
		var node Node
		var itemCount int64
		err := rows.Scan(
			&node.FullPath,
			&node.Name,
			&node.Size,
			&node.Usage,
			&node.IsDir,
			&itemCount,
			&node.Depth,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		node.ItemCount = itemCount
		if !node.IsDir {
			node.FileType = getFileType(node.Name)
			node.Extension = getExtension(node.Name)
		} else {
			node.FileType = "dir"
		}

		children = append(children, &node)
	}

	return children, nil
}

// GetLargestFiles returns the top N largest files in the entire dataset.
func (db *DB) GetLargestFiles(limit int) ([]*Node, error) {
	query := fmt.Sprintf(`
		SELECT path, name, size, usage, is_dir, item_count, depth
		FROM '%s'
		WHERE NOT is_dir
		ORDER BY usage DESC
		LIMIT ?
	`, db.parquetPath)

	rows, err := db.conn.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query largest files: %w", err)
	}
	defer rows.Close()

	var files []*Node
	for rows.Next() {
		var node Node
		var itemCount int64
		err := rows.Scan(
			&node.FullPath,
			&node.Name,
			&node.Size,
			&node.Usage,
			&node.IsDir,
			&itemCount,
			&node.Depth,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		node.ItemCount = itemCount
		node.FileType = getFileType(node.Name)
		node.Extension = getExtension(node.Name)
		files = append(files, &node)
	}

	return files, nil
}

// GetFlatFiles returns all files (non-directories) ordered by usage, with a limit.
func (db *DB) GetFlatFiles(limit int) ([]*Node, error) {
	query := fmt.Sprintf(`
		SELECT path, name, size, usage, is_dir, item_count, depth
		FROM '%s'
		WHERE NOT is_dir
		ORDER BY usage DESC
		LIMIT ?
	`, db.parquetPath)

	rows, err := db.conn.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query flat files: %w", err)
	}
	defer rows.Close()

	var files []*Node
	for rows.Next() {
		var node Node
		var itemCount int64
		err := rows.Scan(
			&node.FullPath,
			&node.Name,
			&node.Size,
			&node.Usage,
			&node.IsDir,
			&itemCount,
			&node.Depth,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		node.ItemCount = itemCount
		node.FileType = getFileType(node.Name)
		node.Extension = getExtension(node.Name)
		files = append(files, &node)
	}

	return files, nil
}

// GetTypeStats returns file type statistics grouped by extension.
func (db *DB) GetTypeStats() ([]FileTypeStat, error) {
	// First, get extension-level stats
	query := fmt.Sprintf(`
		SELECT
			COALESCE(
				CASE
					WHEN name LIKE '%%.%%' THEN LOWER(SUBSTRING(name FROM LENGTH(name) - POSITION('.' IN REVERSE(name)) + 1))
					ELSE '(none)'
				END,
				'(none)'
			) as ext,
			SUM(usage) as total_usage,
			COUNT(*) as file_count
		FROM '%s'
		WHERE NOT is_dir
		GROUP BY ext
		ORDER BY total_usage DESC
	`, db.parquetPath)

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query type stats: %w", err)
	}
	defer rows.Close()

	// Aggregate by file type category
	typeMap := make(map[string]*FileTypeStat)

	for rows.Next() {
		var ext string
		var totalUsage int64
		var fileCount int

		err := rows.Scan(&ext, &totalUsage, &fileCount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Map extension to file type category
		fileType := getFileType("file" + ext)

		if _, ok := typeMap[fileType]; !ok {
			typeMap[fileType] = &FileTypeStat{Type: fileType}
		}
		typeMap[fileType].Size += totalUsage
		typeMap[fileType].Count += fileCount
	}

	// Convert to slice and sort by size
	var stats []FileTypeStat
	for _, stat := range typeMap {
		stats = append(stats, *stat)
	}

	// Sort by size descending
	for i := 0; i < len(stats)-1; i++ {
		for j := i + 1; j < len(stats); j++ {
			if stats[j].Size > stats[i].Size {
				stats[i], stats[j] = stats[j], stats[i]
			}
		}
	}

	return stats, nil
}

// GetExtensionStats returns detailed stats by file extension.
func (db *DB) GetExtensionStats(limit int) ([]FileTypeStat, error) {
	query := fmt.Sprintf(`
		SELECT
			COALESCE(
				CASE
					WHEN name LIKE '%%.%%' THEN LOWER(SUBSTRING(name FROM LENGTH(name) - POSITION('.' IN REVERSE(name)) + 1))
					ELSE '(none)'
				END,
				'(none)'
			) as ext,
			SUM(usage) as total_usage,
			COUNT(*) as file_count
		FROM '%s'
		WHERE NOT is_dir
		GROUP BY ext
		ORDER BY total_usage DESC
		LIMIT ?
	`, db.parquetPath)

	rows, err := db.conn.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query extension stats: %w", err)
	}
	defer rows.Close()

	var stats []FileTypeStat
	for rows.Next() {
		var stat FileTypeStat
		err := rows.Scan(&stat.Type, &stat.Size, &stat.Count)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

// GetCurrentDirStats returns usage stats for a directory.
func (db *DB) GetCurrentDirStats(path string) (size int64, itemCount int, err error) {
	query := fmt.Sprintf(`
		SELECT usage, item_count
		FROM '%s'
		WHERE path = ?
	`, db.parquetPath)

	var ic int64
	err = db.conn.QueryRow(query, path).Scan(&size, &ic)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get dir stats: %w", err)
	}

	return size, int(ic), nil
}

// pathExt extracts the file extension from a path.
func pathExt(path string) string {
	return filepath.Ext(path)
}
