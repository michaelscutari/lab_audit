package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ViewMode represents the current view mode.
type ViewMode int

const (
	ViewTree ViewMode = iota
	ViewFlat
	ViewTypes
	ViewLargest
)

func (v ViewMode) String() string {
	return [...]string{"[D] Tree", "[=] Flat", "[#] Types", "[!] Largest"}[v]
}

// SortMode represents the current sort mode.
type SortMode int

const (
	SortSize SortMode = iota
	SortName
	SortType
	SortCount
)

func (s SortMode) String() string {
	return [...]string{"Size v", "Name ^", "Type", "Items v"}[s]
}

// Key bindings
type keyMap struct {
	Up       key.Binding
	Down     key.Binding
	Left     key.Binding
	Right    key.Binding
	Enter    key.Binding
	Back     key.Binding
	Search   key.Binding
	View     key.Binding
	Sort     key.Binding
	Help     key.Binding
	Quit     key.Binding
	PageUp   key.Binding
	PageDown key.Binding
	Top      key.Binding
	Bottom   key.Binding
	Escape   key.Binding
}

var keys = keyMap{
	Up:       key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("^/k", "up")),
	Down:     key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("v/j", "down")),
	Left:     key.NewBinding(key.WithKeys("left", "h"), key.WithHelp("<-/h", "back")),
	Right:    key.NewBinding(key.WithKeys("right", "l"), key.WithHelp("->/l", "open")),
	Enter:    key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "open")),
	Back:     key.NewBinding(key.WithKeys("backspace"), key.WithHelp("bksp", "back")),
	Search:   key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
	View:     key.NewBinding(key.WithKeys("v"), key.WithHelp("v", "view mode")),
	Sort:     key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "sort")),
	Help:     key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
	Quit:     key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	PageUp:   key.NewBinding(key.WithKeys("pgup", "ctrl+u"), key.WithHelp("pgup", "page up")),
	PageDown: key.NewBinding(key.WithKeys("pgdown", "ctrl+d"), key.WithHelp("pgdn", "page down")),
	Top:      key.NewBinding(key.WithKeys("g", "home"), key.WithHelp("g", "top")),
	Bottom:   key.NewBinding(key.WithKeys("G", "end"), key.WithHelp("G", "bottom")),
	Escape:   key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "cancel")),
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Enter, k.Back, k.Search, k.View, k.Help, k.Quit}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.PageUp, k.PageDown, k.Top, k.Bottom},
		{k.Enter, k.Right, k.Back, k.Left},
		{k.Search, k.View, k.Sort},
		{k.Help, k.Quit, k.Escape},
	}
}

// model is the Bubble Tea model for the TUI.
type model struct {
	// Database connection
	db *DB

	// Cache for directory children
	cache *DirCache

	// Navigation state (path-based instead of Node pointers)
	rootPath    string   // Root path from parquet
	currentPath string   // Current directory path
	stack       []string // Navigation history (path strings)

	// Current view data (lazy loaded)
	children     []*Node // Children of current directory
	currentUsage int64   // Usage of current directory

	// View mode data
	flatList     []*Node        // Flat file list (lazy loaded)
	largestFiles []*Node        // Top 100 largest files (lazy loaded)
	typeStats    []FileTypeStat // File type statistics (lazy loaded)

	// Global stats (loaded once at startup)
	stats *GlobalStats

	// UI state
	cursor      int
	offset      int // For scrolling
	width       int
	height      int
	viewMode    ViewMode
	sortMode    SortMode
	showHelp    bool
	searching   bool
	searchInput textinput.Model
	searchQuery string
	filtered    []*Node
	help        help.Model
	fileName    string

	// Error state
	err error
}

// initialModel creates the initial model.
func initialModel(db *DB, stats *GlobalStats, rootPath, fileName string) model {
	ti := textinput.New()
	ti.Placeholder = "Search files..."
	ti.CharLimit = 100
	ti.Width = 40
	ti.PromptStyle = lipgloss.NewStyle().Foreground(colorWarning)
	ti.TextStyle = lipgloss.NewStyle().Foreground(colorTextBright)

	h := help.New()
	h.ShowAll = false
	h.Styles.ShortKey = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
	h.Styles.ShortDesc = lipgloss.NewStyle().Foreground(colorTextDim)
	h.Styles.FullKey = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
	h.Styles.FullDesc = lipgloss.NewStyle().Foreground(colorTextDim)

	return model{
		db:          db,
		cache:       NewDirCache(100),
		rootPath:    rootPath,
		currentPath: rootPath,
		stack:       make([]string, 0),
		stats:       stats,
		searchInput: ti,
		help:        h,
		fileName:    fileName,
		width:       120,
		height:      40,
	}
}

// Init is the Bubble Tea init function.
func (m model) Init() tea.Cmd {
	return m.loadCurrentDirectory
}

// loadCurrentDirectory loads children for the current directory.
func (m model) loadCurrentDirectory() tea.Msg {
	children, err := m.getChildren(m.currentPath)
	if err != nil {
		return errMsg{err}
	}

	// Get current directory usage
	usage, _, err := m.db.GetCurrentDirStats(m.currentPath)
	if err != nil {
		return errMsg{err}
	}

	return childrenLoadedMsg{children: children, usage: usage}
}

// getChildren returns children from cache or queries the database.
func (m *model) getChildren(path string) ([]*Node, error) {
	// Check cache first
	if cached := m.cache.Get(path); cached != nil {
		return cached, nil
	}

	// Query database
	children, err := m.db.GetChildren(path)
	if err != nil {
		return nil, err
	}

	// Cache the result
	m.cache.Set(path, children)

	return children, nil
}

// Message types
type childrenLoadedMsg struct {
	children []*Node
	usage    int64
}

type flatListLoadedMsg struct {
	files []*Node
}

type largestLoadedMsg struct {
	files []*Node
}

type typeStatsLoadedMsg struct {
	stats []FileTypeStat
}

type errMsg struct {
	err error
}

// Update is the Bubble Tea update function.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case childrenLoadedMsg:
		m.children = msg.children
		m.currentUsage = msg.usage
		m.applySort()
		return m, nil

	case flatListLoadedMsg:
		m.flatList = msg.files
		return m, nil

	case largestLoadedMsg:
		m.largestFiles = msg.files
		return m, nil

	case typeStatsLoadedMsg:
		m.typeStats = msg.stats
		return m, nil

	case errMsg:
		m.err = msg.err
		return m, nil
	}

	// Handle search input mode
	if m.searching {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "enter":
				m.searching = false
				m.searchQuery = m.searchInput.Value()
				m.applySearch()
				m.cursor = 0
				m.offset = 0
			case "esc":
				m.searching = false
				m.searchInput.SetValue("")
				m.searchQuery = ""
				m.filtered = nil
			default:
				m.searchInput, cmd = m.searchInput.Update(msg)
				return m, cmd
			}
		}
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, keys.Quit):
			return m, tea.Quit

		case key.Matches(msg, keys.Help):
			m.showHelp = !m.showHelp

		case key.Matches(msg, keys.Escape):
			if m.searchQuery != "" {
				m.searchQuery = ""
				m.searchInput.SetValue("")
				m.filtered = nil
				m.cursor = 0
				m.offset = 0
			}

		case key.Matches(msg, keys.Up):
			if m.cursor > 0 {
				m.cursor--
				m.ensureVisible()
			}

		case key.Matches(msg, keys.Down):
			maxCursor := m.getMaxCursor()
			if m.cursor < maxCursor-1 {
				m.cursor++
				m.ensureVisible()
			}

		case key.Matches(msg, keys.PageUp):
			visible := m.visibleRows()
			m.cursor -= visible
			if m.cursor < 0 {
				m.cursor = 0
			}
			m.ensureVisible()

		case key.Matches(msg, keys.PageDown):
			visible := m.visibleRows()
			maxCursor := m.getMaxCursor()
			m.cursor += visible
			if m.cursor >= maxCursor {
				m.cursor = maxCursor - 1
			}
			m.ensureVisible()

		case key.Matches(msg, keys.Top):
			m.cursor = 0
			m.offset = 0

		case key.Matches(msg, keys.Bottom):
			m.cursor = m.getMaxCursor() - 1
			m.ensureVisible()

		case key.Matches(msg, keys.Enter), key.Matches(msg, keys.Right):
			if m.viewMode == ViewTree || m.viewMode == ViewLargest {
				selected := m.getSelected()
				if selected != nil && selected.IsDir {
					m.stack = append(m.stack, m.currentPath)
					m.currentPath = selected.FullPath
					m.cursor = 0
					m.offset = 0
					m.filtered = nil
					m.searchQuery = ""
					m.searchInput.SetValue("")
					return m, m.loadCurrentDirectory
				}
			}

		case key.Matches(msg, keys.Back), key.Matches(msg, keys.Left):
			if m.viewMode == ViewTree {
				if len(m.stack) > 0 {
					m.currentPath = m.stack[len(m.stack)-1]
					m.stack = m.stack[:len(m.stack)-1]
					m.cursor = 0
					m.offset = 0
					m.filtered = nil
					m.searchQuery = ""
					m.searchInput.SetValue("")
					return m, m.loadCurrentDirectory
				}
			}

		case key.Matches(msg, keys.Search):
			m.searching = true
			m.searchInput.Focus()
			return m, textinput.Blink

		case key.Matches(msg, keys.View):
			oldMode := m.viewMode
			m.viewMode = ViewMode((int(m.viewMode) + 1) % 4)
			m.cursor = 0
			m.offset = 0

			// Load data for new view if needed
			switch m.viewMode {
			case ViewFlat:
				if m.flatList == nil {
					return m, m.loadFlatList
				}
			case ViewLargest:
				if m.largestFiles == nil {
					return m, m.loadLargestFiles
				}
			case ViewTypes:
				if m.typeStats == nil {
					return m, m.loadTypeStats
				}
			case ViewTree:
				if oldMode != ViewTree {
					// Refresh tree view
					return m, m.loadCurrentDirectory
				}
			}

		case key.Matches(msg, keys.Sort):
			m.sortMode = SortMode((int(m.sortMode) + 1) % 4)
			m.applySort()
			m.cursor = 0
			m.offset = 0
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.help.Width = msg.Width
	}

	return m, nil
}

// loadFlatList loads the flat file list.
func (m model) loadFlatList() tea.Msg {
	files, err := m.db.GetFlatFiles(1000)
	if err != nil {
		return errMsg{err}
	}
	return flatListLoadedMsg{files: files}
}

// loadLargestFiles loads the largest files.
func (m model) loadLargestFiles() tea.Msg {
	files, err := m.db.GetLargestFiles(100)
	if err != nil {
		return errMsg{err}
	}
	return largestLoadedMsg{files: files}
}

// loadTypeStats loads file type statistics.
func (m model) loadTypeStats() tea.Msg {
	stats, err := m.db.GetTypeStats()
	if err != nil {
		return errMsg{err}
	}
	return typeStatsLoadedMsg{stats: stats}
}

func (m *model) ensureVisible() {
	visible := m.visibleRows()
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+visible {
		m.offset = m.cursor - visible + 1
	}
}

func (m model) visibleRows() int {
	return max(1, m.height-16)
}

func (m model) getMaxCursor() int {
	switch m.viewMode {
	case ViewTree:
		if m.filtered != nil {
			return len(m.filtered)
		}
		return len(m.children)
	case ViewFlat:
		if m.filtered != nil {
			return len(m.filtered)
		}
		return len(m.flatList)
	case ViewTypes:
		return len(m.typeStats)
	case ViewLargest:
		return len(m.largestFiles)
	}
	return 0
}

func (m model) getSelected() *Node {
	switch m.viewMode {
	case ViewTree:
		list := m.children
		if m.filtered != nil {
			list = m.filtered
		}
		if m.cursor >= 0 && m.cursor < len(list) {
			return list[m.cursor]
		}
	case ViewFlat:
		list := m.flatList
		if m.filtered != nil {
			list = m.filtered
		}
		if m.cursor >= 0 && m.cursor < len(list) {
			return list[m.cursor]
		}
	case ViewLargest:
		if m.cursor >= 0 && m.cursor < len(m.largestFiles) {
			return m.largestFiles[m.cursor]
		}
	}
	return nil
}

func (m *model) applySearch() {
	if m.searchQuery == "" {
		m.filtered = nil
		return
	}

	query := strings.ToLower(m.searchQuery)
	var source []*Node

	switch m.viewMode {
	case ViewTree:
		source = m.children
	case ViewFlat, ViewLargest:
		source = m.flatList
	default:
		return
	}

	m.filtered = make([]*Node, 0)
	for _, n := range source {
		if strings.Contains(strings.ToLower(n.Name), query) ||
			strings.Contains(strings.ToLower(n.FullPath), query) {
			m.filtered = append(m.filtered, n)
		}
	}
}

func (m *model) applySort() {
	sortNodes := func(nodes []*Node) {
		switch m.sortMode {
		case SortSize:
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].Usage > nodes[j].Usage
			})
		case SortName:
			sort.Slice(nodes, func(i, j int) bool {
				return strings.ToLower(nodes[i].Name) < strings.ToLower(nodes[j].Name)
			})
		case SortType:
			sort.Slice(nodes, func(i, j int) bool {
				if nodes[i].FileType != nodes[j].FileType {
					return nodes[i].FileType < nodes[j].FileType
				}
				return nodes[i].Usage > nodes[j].Usage
			})
		case SortCount:
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].ItemCount > nodes[j].ItemCount
			})
		}
	}

	if m.children != nil {
		sortNodes(m.children)
	}
	if m.flatList != nil {
		sortNodes(m.flatList)
	}
}

// View is the Bubble Tea view function.
func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	if m.err != nil {
		return lipgloss.NewStyle().
			Foreground(colorDanger).
			Render("Error: " + m.err.Error())
	}

	var b strings.Builder

	// Header
	b.WriteString(m.renderHeader())
	b.WriteString("\n")

	// Stats bar
	b.WriteString(m.renderStatsBar())
	b.WriteString("\n")

	// Search bar (if searching)
	if m.searching {
		b.WriteString(m.renderSearchBar())
		b.WriteString("\n")
	} else if m.searchQuery != "" {
		searchInfo := lipgloss.NewStyle().
			Foreground(colorWarning).
			Render(fmt.Sprintf(">> Filtering: \"%s\" (%d results) [ESC to clear]",
				m.searchQuery, len(m.filtered)))
		b.WriteString(searchInfo)
		b.WriteString("\n")
	}

	// Main content
	b.WriteString(m.renderContent())

	// Help bar
	b.WriteString("\n")
	b.WriteString(m.renderHelpBar())

	return b.String()
}
