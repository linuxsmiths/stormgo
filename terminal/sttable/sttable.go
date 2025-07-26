package sttable

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

type SortOrder int

const (
	SortOrderNone SortOrder = iota // No sorting applied.
	SortOrderAsc                   // Ascending order.
	SortOrderDesc                  // Descending order.
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

	//
	// Sorting order for this cell, if applicable.
	// Only the header cells can have a sorting order.
	//
	Sort SortOrder
}

// This represents one row of STTable.
// A row holds data for a single record and is a collection of cells where
// each cell holds one attribute of that entity.
type STRow struct {
	Cells []STCell
}

// STCol defines a column that can be added to a STTable.
type STCol struct {
	//
	// Id is the unique identifier for the column that identifies this column
	// from various available columns. Every column that can be added to any
	// table must have a globally unique Id. This is used to fetch the column
	// metadata including deciding how the column data is populated. This is
	// not displayed in the table.
	//
	Id string

	//
	// Description of the column, not displayed in the table.
	//
	Desc string

	//
	// Is this column a key column?
	// A key column has unique values for each row in the table.
	// There can be only one key column in a table.
	//
	IsKey bool

	//
	// Every column must have a header that's displayed at the top row.
	//
	Header string

	//
	// How does this column get its data?
	//
	Source string

	//
	// How many characters must be used to display this column?
	// Both header and row width is controlled by this.
	//
	Width int
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

	//
	// All the columns of the table.
	// Each column produces one header which gets added to Header and one or more
	// rows which get added to Rows.
	//
	Cols []*STCol
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

// Sort the table based on the content of the column at colIdx.
func (st *STTable) Sort(colIdx int, order SortOrder) {
	log.Assert(order == SortOrderAsc || order == SortOrderDesc, order)

	if order == SortOrderAsc {
		sort.Slice(st.Rows, func(i, j int) bool {
			return st.Rows[i].Cells[colIdx].Content < st.Rows[j].Cells[colIdx].Content
		})
	} else {
		sort.Slice(st.Rows, func(i, j int) bool {
			return st.Rows[i].Cells[colIdx].Content > st.Rows[j].Cells[colIdx].Content
		})
	}
}

// JSON column definition.
type ColDef struct {
	Desc   string `json:"desc"`
	Id     string `json:"id"`
	Name   string `json:"name"`
	Source string `json:"source"`
	IsKey  bool   `json:"iskey"`
}

// This function parses a column definition and returns a correctly populated
// STCol object.
func ParseColDef(colDefJsonFile string) (*STCol, error) {
	file, err := os.Open(colDefJsonFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", colDefJsonFile, err)
	}
	defer file.Close()

	// Variable to hold the decoded data.
	var colDef ColDef

	// Decode JSON from file into the struct
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&colDef)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON from %s: %v", colDefJsonFile, err)
	}

	col := &STCol{
		Id:     colDef.Id,
		Desc:   colDef.Desc,
		IsKey:  colDef.IsKey,
		Header: colDef.Name,
		Source: colDef.Source,
		Width:  10,
	}

	return col, nil
}

func (st *STTable) AddCol(col *STCol) {
	st.Cols = append(st.Cols, col)
}

// Generate rows for this table based on the column definitions added to the
// table.
func (st *STTable) GenRows() {
	// Columns must be added to generate rows.
	log.Assert(len(st.Cols) > 0)

	for col := range st.Cols {
	}
}
