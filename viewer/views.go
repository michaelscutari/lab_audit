package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// renderHeader renders the top header bar with title, breadcrumb, and badges.
func (m model) renderHeader() string {
	title := titleStyle.Render(" GDU ANALYZER ")

	breadcrumb := m.renderBreadcrumb()

	viewBadge := lipgloss.NewStyle().
		Background(colorBgHighlight).
		Foreground(colorAccent).
		Padding(0, 1).
		Bold(true).
		Render(m.viewMode.String())

	sortBadge := lipgloss.NewStyle().
		Background(colorBgHighlight).
		Foreground(colorWarning).
		Padding(0, 1).
		Render(":: " + m.sortMode.String())

	header := lipgloss.JoinHorizontal(lipgloss.Center,
		title, "  ", breadcrumb, "  ", viewBadge, " ", sortBadge)

	return header
}

// renderBreadcrumb renders the path breadcrumb.
func (m model) renderBreadcrumb() string {
	parts := strings.Split(strings.TrimPrefix(m.currentPath, "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		parts = []string{"ROOT"}
	}

	var result []string
	for i, p := range parts {
		if i == len(parts)-1 {
			result = append(result, breadcrumbActiveStyle.Render(p))
		} else {
			result = append(result, breadcrumbStyle.Render(p))
		}
	}

	return strings.Join(result, breadcrumbStyle.Render(" > "))
}

// renderStatsBar renders the statistics bar below the header.
func (m model) renderStatsBar() string {
	currentSize := m.currentUsage
	currentItems := len(m.children)

	stats := []string{
		statLabelStyle.Render("Total: ") + statValueStyle.Render(humanize(m.stats.TotalUsage)),
		statLabelStyle.Render("Files: ") + statValueStyle.Render(formatNumber(int(m.stats.TotalFiles))),
		statLabelStyle.Render("Dirs: ") + statValueStyle.Render(formatNumber(int(m.stats.TotalDirs))),
		statLabelStyle.Render("Current: ") + statValueStyle.Render(humanize(currentSize)),
		statLabelStyle.Render("Items: ") + statValueStyle.Render(formatNumber(currentItems)),
	}

	return lipgloss.NewStyle().
		Foreground(colorTextDim).
		Render("<- ") + strings.Join(stats, "  |  ") + lipgloss.NewStyle().
		Foreground(colorTextDim).
		Render(" ->")
}

// renderSearchBar renders the search input.
func (m model) renderSearchBar() string {
	return searchStyle.Render(">> " + m.searchInput.View())
}

// renderContent dispatches to the appropriate view renderer.
func (m model) renderContent() string {
	switch m.viewMode {
	case ViewTree:
		return m.renderTreeView()
	case ViewFlat:
		return m.renderFlatView()
	case ViewTypes:
		return m.renderTypesView()
	case ViewLargest:
		return m.renderLargestView()
	}
	return ""
}

// renderTreeView renders the tree navigation view.
func (m model) renderTreeView() string {
	list := m.children
	if m.filtered != nil {
		list = m.filtered
	}

	return m.renderNodeList(list, m.currentUsage)
}

// renderFlatView renders the flat file list view.
func (m model) renderFlatView() string {
	list := m.flatList
	if m.filtered != nil {
		list = m.filtered
	}

	return m.renderNodeList(list, m.stats.TotalUsage)
}

// renderLargestView renders the largest files view.
func (m model) renderLargestView() string {
	var b strings.Builder

	header := lipgloss.NewStyle().
		Foreground(colorOrange).
		Bold(true).
		Render(">> TOP 100 LARGEST FILES")
	b.WriteString(header)
	b.WriteString("\n\n")

	b.WriteString(m.renderNodeList(m.largestFiles, m.stats.TotalUsage))
	return b.String()
}

// renderNodeList renders a list of nodes with size bars.
func (m model) renderNodeList(list []*Node, parentSize int64) string {
	var b strings.Builder

	visible := m.visibleRows()
	start := m.offset
	end := min(start+visible, len(list))

	// Column header
	header := lipgloss.NewStyle().
		Foreground(colorTextDim).
		Bold(true).
		Render(fmt.Sprintf("  %-3s  %-40s  %10s  %6s  %-20s",
			"", "NAME", "SIZE", "%", "USAGE"))
	b.WriteString(header)
	b.WriteString("\n")

	separator := lipgloss.NewStyle().
		Foreground(colorBorder).
		Render(strings.Repeat("-", min(m.width-4, 90)))
	b.WriteString(separator)
	b.WriteString("\n")

	for i := start; i < end; i++ {
		node := list[i]
		isSelected := i == m.cursor

		icon := m.getIcon(node)

		pct := float64(0)
		if parentSize > 0 {
			pct = float64(node.Usage) / float64(parentSize) * 100
		}

		bar := m.renderBar(pct, 20)

		name := truncate(node.Name, 40)
		if node.IsDir {
			name = name + "/"
		}

		line := fmt.Sprintf("%-3s  %-40s  %10s  %5.1f%%  %s",
			icon, name, humanize(node.Usage), pct, bar)

		var style lipgloss.Style
		if isSelected {
			style = selectedStyle
		} else {
			style = normalStyle
		}

		cursor := "  "
		if isSelected {
			cursor = lipgloss.NewStyle().Foreground(colorAccent).Bold(true).Render("> ")
		}

		b.WriteString(cursor)
		b.WriteString(style.Render(line))
		b.WriteString("\n")
	}

	// Scroll indicator
	if len(list) > visible {
		scrollInfo := lipgloss.NewStyle().
			Foreground(colorTextDim).
			Render(fmt.Sprintf("\n  [%d-%d of %d]", start+1, end, len(list)))
		b.WriteString(scrollInfo)
	}

	return b.String()
}

// renderTypesView renders the file type breakdown view.
func (m model) renderTypesView() string {
	var b strings.Builder

	header := lipgloss.NewStyle().
		Foreground(colorPurple).
		Bold(true).
		Render(">> FILE TYPE BREAKDOWN")
	b.WriteString(header)
	b.WriteString("\n\n")

	colHeader := lipgloss.NewStyle().
		Foreground(colorTextDim).
		Bold(true).
		Render(fmt.Sprintf("  %-15s  %10s  %8s  %6s  %-30s",
			"TYPE", "SIZE", "COUNT", "%", "DISTRIBUTION"))
	b.WriteString(colHeader)
	b.WriteString("\n")

	separator := lipgloss.NewStyle().
		Foreground(colorBorder).
		Render(strings.Repeat("-", min(m.width-4, 85)))
	b.WriteString(separator)
	b.WriteString("\n")

	visible := m.visibleRows()
	start := m.offset
	end := min(start+visible, len(m.typeStats))

	for i := start; i < end; i++ {
		ts := m.typeStats[i]
		isSelected := i == m.cursor

		pct := float64(ts.Size) / float64(m.stats.TotalUsage) * 100
		color := fileTypeColors[ts.Type]
		if color == "" {
			color = colorTextDim
		}
		bar := m.renderBarColored(pct, 30, color)

		line := fmt.Sprintf("%-15s  %10s  %8d  %5.1f%%  %s",
			ts.Type, humanize(ts.Size), ts.Count, pct, bar)

		cursor := "  "
		if isSelected {
			cursor = lipgloss.NewStyle().Foreground(colorAccent).Bold(true).Render("> ")
			line = selectedStyle.Render(line)
		} else {
			line = normalStyle.Render(line)
		}

		b.WriteString(cursor)
		b.WriteString(line)
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(m.renderMiniChart())

	return b.String()
}

// renderMiniChart renders a distribution bar chart.
func (m model) renderMiniChart() string {
	var b strings.Builder

	chartTitle := lipgloss.NewStyle().
		Foreground(colorTextDim).
		Bold(true).
		Render("  Distribution Overview:")
	b.WriteString(chartTitle)
	b.WriteString("\n  ")

	totalWidth := min(m.width-10, 80)
	remaining := totalWidth

	for _, ts := range m.typeStats {
		pct := float64(ts.Size) / float64(m.stats.TotalUsage)
		width := int(pct * float64(totalWidth))
		if width < 1 && pct > 0 {
			width = 1
		}
		if width > remaining {
			width = remaining
		}
		remaining -= width

		color := fileTypeColors[ts.Type]
		if color == "" {
			color = colorTextDim
		}

		segment := lipgloss.NewStyle().
			Background(color).
			Foreground(colorBg).
			Render(strings.Repeat(" ", width))
		b.WriteString(segment)
	}

	if remaining > 0 {
		b.WriteString(lipgloss.NewStyle().Background(colorBgAlt).Render(strings.Repeat(" ", remaining)))
	}

	return b.String()
}

// renderBar renders a usage bar with color based on percentage.
func (m model) renderBar(pct float64, width int) string {
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}

	var color lipgloss.Color
	switch {
	case pct > 50:
		color = colorDanger
	case pct > 25:
		color = colorWarning
	case pct > 10:
		color = colorAccent
	default:
		color = colorSuccess
	}

	bar := lipgloss.NewStyle().Foreground(color).Render(strings.Repeat(barFull, filled))
	empty := lipgloss.NewStyle().Foreground(colorBgHighlight).Render(strings.Repeat(barEmpty, width-filled))

	return bar + empty
}

// renderBarColored renders a usage bar with a specific color.
func (m model) renderBarColored(pct float64, width int, color lipgloss.Color) string {
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}

	bar := lipgloss.NewStyle().Foreground(color).Render(strings.Repeat(barFull, filled))
	empty := lipgloss.NewStyle().Foreground(colorBgHighlight).Render(strings.Repeat(barEmpty, width-filled))

	return bar + empty
}

// getIcon returns an icon for a node based on its type.
func (m model) getIcon(node *Node) string {
	if node.IsDir {
		return "[D]"
	}

	switch node.FileType {
	case "code":
		return "[C]"
	case "data":
		return "[#]"
	case "media":
		return "[M]"
	case "archive":
		return "[Z]"
	case "doc":
		return "[T]"
	case "config":
		return "[*]"
	default:
		return "[.]"
	}
}

// renderHelpBar renders the help/shortcuts bar.
func (m model) renderHelpBar() string {
	if m.showHelp {
		return m.help.View(keys)
	}

	shortcuts := []string{
		lipgloss.NewStyle().Foreground(colorAccent).Render("^/v") + " nav",
		lipgloss.NewStyle().Foreground(colorAccent).Render("enter") + " open",
		lipgloss.NewStyle().Foreground(colorAccent).Render("bksp") + " back",
		lipgloss.NewStyle().Foreground(colorAccent).Render("/") + " search",
		lipgloss.NewStyle().Foreground(colorAccent).Render("v") + " view",
		lipgloss.NewStyle().Foreground(colorAccent).Render("s") + " sort",
		lipgloss.NewStyle().Foreground(colorAccent).Render("?") + " help",
		lipgloss.NewStyle().Foreground(colorAccent).Render("q") + " quit",
	}

	return helpStyle.Render(strings.Join(shortcuts, "  |  "))
}

// Helper functions

func truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

func humanize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatNumber(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1000000)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
