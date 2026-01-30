package main

import "github.com/charmbracelet/lipgloss"

// Color Palette - Industrial Cyberpunk
var (
	colorBg           = lipgloss.Color("#0d1117")
	colorBgAlt        = lipgloss.Color("#161b22")
	colorBgHighlight  = lipgloss.Color("#21262d")
	colorBorder       = lipgloss.Color("#30363d")
	colorBorderBright = lipgloss.Color("#484f58")
	colorText         = lipgloss.Color("#c9d1d9")
	colorTextDim      = lipgloss.Color("#8b949e")
	colorTextBright   = lipgloss.Color("#f0f6fc")
	colorAccent       = lipgloss.Color("#58a6ff")
	colorAccentBright = lipgloss.Color("#79c0ff")
	colorSuccess      = lipgloss.Color("#3fb950")
	colorWarning      = lipgloss.Color("#d29922")
	colorDanger       = lipgloss.Color("#f85149")
	colorPurple       = lipgloss.Color("#bc8cff")
	colorCyan         = lipgloss.Color("#39c5cf")
	colorOrange       = lipgloss.Color("#ffa657")
	colorPink         = lipgloss.Color("#ff7b72")
)

// File Type Colors
var fileTypeColors = map[string]lipgloss.Color{
	"code":    colorSuccess,
	"data":    colorAccent,
	"media":   colorPurple,
	"archive": colorOrange,
	"doc":     colorCyan,
	"config":  colorWarning,
	"other":   colorTextDim,
	"dir":     colorAccentBright,
}

// Styles
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorTextBright).
			Background(colorAccent).
			Padding(0, 2)

	subtitleStyle = lipgloss.NewStyle().
			Foreground(colorAccent).
			Bold(true)

	breadcrumbStyle = lipgloss.NewStyle().
			Foreground(colorTextDim)

	breadcrumbActiveStyle = lipgloss.NewStyle().
				Foreground(colorAccent).
				Bold(true)

	statLabelStyle = lipgloss.NewStyle().
			Foreground(colorTextDim)

	statValueStyle = lipgloss.NewStyle().
			Foreground(colorTextBright).
			Bold(true)

	selectedStyle = lipgloss.NewStyle().
			Background(colorBgHighlight).
			Foreground(colorTextBright).
			Bold(true)

	normalStyle = lipgloss.NewStyle().
			Foreground(colorText)

	dimStyle = lipgloss.NewStyle().
			Foreground(colorTextDim)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(1, 2)

	activePanelStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorAccent).
				Padding(1, 2)

	helpStyle = lipgloss.NewStyle().
			Foreground(colorTextDim)

	searchStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorWarning).
			Padding(0, 1)
)

// Bar Characters
const (
	barFull  = "█"
	barHalf  = "▌"
	barEmpty = "░"
)
