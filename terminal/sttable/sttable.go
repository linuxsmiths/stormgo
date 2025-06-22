package sttable

import (
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

// A cell is the smallest unit of data in a table.
// A table is made of multiple rows and each row is made of multiple cells
// with each cell holding one attribute of the entity represented by the row.
type STCell struct {
	// Cell data.
	Content string

	//
	// The width of the cell in characters.
	// Only these many characters are displayed in this cell.
	// If Content is longer, it is truncated and if it's shorter, it's padded
	// with spaces.
	//
	Width int
}

// This represents one row of STTable.
// A row holds data for a single record and is a collection of cells where
// each cell holds one attribute of that entity.
type STRow struct {
	Cells []STCell
}

// This represents a table of data that's displayed in a STWin.
// This is the Model part of the MVC pattern, the View is the STWin that
// displays the table.
type STTable struct {
	//
	// Table name, this becomes the title of the window that displays this
	// table.
	//
	Name string

	//
	// The header for the table. This is a single row that contains info on
	// all the columns displayed in the table.
	// This is displayed at the top of the table.
	// Each cell in the header is clickable and clicking on it will sort the
	// Rows in the table based on the content of that column.
	//
	Header STRow

	//
	// Total width in characters needed to display the table.
	//
	Width int

	//
	// Various rows of the table.
	// These are displayed below the header.
	// Total number of rows displayed depends on the height of the containing
	// STWin.
	//
	Rows []STRow
}

func NewTable(name string) *STTable {
	log.Assert(name != "", "empty table name not allowed")

	return &STTable{
		Name: name,
	}
}

// Add header to a table.
func (st *STTable) AddHeader(cells []STCell) {
	// Header must have at least one cell.
	log.Assert(len(cells) > 0, st.Name)
	// Must be set only once.
	log.Assert(len(st.Header.Cells) == 0, len(st.Header.Cells), st.Name)
	// Header must be set before adding rows to the table.
	log.Assert(len(st.Rows) == 0, len(st.Rows), st.Name)
	log.Assert(st.Width == 0, st.Width, st.Name)

	for i, cell := range cells {
		// Header must be chosen such that they are fully displayed.
		log.Assert(cell.Width >= len(cell.Content), i, cell.Width, cell.Content, st.Name)
		// +1 for the space between columns.
		st.Width += (cell.Width + 1)
	}

	// The last column does not need a space after it.
	st.Width--

	//
	// Total width of the table must not exceed the maximum number of columns,
	// else the table cannot be displayed properly.
	//
	log.Assert(st.Width <= stlib.GetMaxCols(), st.Width, stlib.GetMaxCols(), st.Name)

	st.Header.Cells = cells
}

// Add a new row to the table.
func (st *STTable) AddRow(cols []string) {
	// Header must be set before adding rows to the table.
	log.Assert(len(st.Header.Cells) > 0, st.Name)
	// The number of columns in the row must match the number of columns in
	// the header.
	log.Assert(len(cols) == len(st.Header.Cells), len(cols), len(st.Header.Cells), st.Name)

	cells := make([]STCell, len(cols))

	for i, col := range cols {
		// Header must have been already set.
		log.Assert(st.Header.Cells[i].Width > 0)

		cells[i] = STCell{Content: col, Width: st.Header.Cells[i].Width}
	}

	st.Rows = append(st.Rows, STRow{Cells: cells})
}

// Get the number of rows in the table.
func (st *STTable) GetRowCount() int {
	// Header must be set when we call this.
	log.Assert(len(st.Header.Cells) > 0, st.Name)

	return len(st.Rows)
}

// Get the number of columns in the table.
func (st *STTable) GetColumnCount() int {
	// Header must be set when we call this.
	log.Assert(len(st.Header.Cells) > 0, st.Name)

	return len(st.Header.Cells)
}
