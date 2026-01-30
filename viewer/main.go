package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println(lipgloss.NewStyle().
			Foreground(colorDanger).
			Bold(true).
			Render("Usage: gdu-view <file.parquet>"))
		os.Exit(1)
	}

	fileName := os.Args[1]

	// Open database
	loadStyle := lipgloss.NewStyle().Foreground(colorAccent)
	fmt.Print(loadStyle.Render("Opening database..."))

	db, err := OpenDB(fileName)
	if err != nil {
		fmt.Printf("\n%s\n", lipgloss.NewStyle().
			Foreground(colorDanger).
			Render(fmt.Sprintf("Failed to open database: %v", err)))
		os.Exit(1)
	}
	defer db.Close()

	fmt.Print("\r" + loadStyle.Render("Loading stats...   "))

	// Load global stats
	stats, err := db.GetGlobalStats()
	if err != nil {
		fmt.Printf("\n%s\n", lipgloss.NewStyle().
			Foreground(colorDanger).
			Render(fmt.Sprintf("Failed to load stats: %v", err)))
		os.Exit(1)
	}

	// Get root path
	rootPath, err := db.GetRootPath()
	if err != nil {
		fmt.Printf("\n%s\n", lipgloss.NewStyle().
			Foreground(colorDanger).
			Render(fmt.Sprintf("Failed to get root: %v", err)))
		os.Exit(1)
	}

	fmt.Printf("\r%s\n", lipgloss.NewStyle().
		Foreground(colorSuccess).
		Render(fmt.Sprintf("Loaded %d items (%d files, %d dirs)",
			stats.TotalItems, stats.TotalFiles, stats.TotalDirs)))

	// Start TUI
	p := tea.NewProgram(
		initialModel(db, stats, rootPath, fileName),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
