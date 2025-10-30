package sttable

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

type SortOrder int

const (
	SortOrderNone SortOrder = iota // No sorting applied.
	SortOrderAsc                   // Ascending order.
	SortOrderDesc                  // Descending order.
)

var (
	// Directory holding column definitions.
	ColumnDefinitionsDir string

	// Directory holding data files used for displaying tables.
	// See data.layout for the structure of this directory.
	DataDir string
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

	//
	// If this table needs to be sorted, the column on which to sort and the
	// sort order. If SortOrder is SortOrderNone, no sorting is applied.
	// Updated and saved when a header cell is clicked.
	//
	SortColIdx int
	SortOrder  SortOrder

	//
	// For dynamic tables, refresh() calls GenRows() to regenerate the rows
	// every time.
	// Non-dynamic tables also can have their data changed through other
	// means and refresh() will just draw the latest table data.
	//
	IsDynamic bool
}

func NewTable(name string, dynamic bool) *STTable {
	log.Assert(name != "", "empty table name not allowed")

	return &STTable{
		Name:       name,
		IsDynamic:  dynamic,
		SortColIdx: -1,
		SortOrder:  SortOrderNone,
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

// Sort the table based on the content of the column at st.SortColIdx and as
// per st.SortOrder.
func (st *STTable) Sort() {
	colIdx := st.SortColIdx
	order := st.SortOrder

	// No sorting requested by user.
	if order == SortOrderNone {
		return
	}

	log.Assert(order == SortOrderAsc || order == SortOrderDesc, order)
	log.Assert(colIdx >= 0 && colIdx < len(st.Header.Cells),
		colIdx, len(st.Header.Cells), st.Name)

	stlib.PrintStatus("Sorting table: %s, by colIdx: %d with order: %d",
		st.Name, colIdx, order)

	// else, sort the table by the clicked column.
	cell := &st.Header.Cells[colIdx]
	cell.Sort = order

	// Only one column can be sorted at a time.
	for i := range st.Header.Cells {
		if i != colIdx {
			st.Header.Cells[i].Sort = SortOrderNone
		}
	}

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

func (st *STTable) AddCol(colDefName string) error {
	col, err := ParseColDef(filepath.Join(ColumnDefinitionsDir, colDefName+".json"))
	if err != nil {
		return fmt.Errorf("failed to parse column definition %s: %v", colDefName, err)
	}

	if st.Cols == nil {
		if !col.IsKey {
			return fmt.Errorf("first column added to table (%s) must be a key column",
				st.Name)
		}
	} else {
		if col.IsKey {
			return fmt.Errorf("table (%s) already has a key column, cannot add %s",
				st.Name, col.Header)
		}
	}

	st.Cols = append(st.Cols, col)

	log.Debugf("Added column %s to table %s, total columns: %d",
		col.Header, st.Name, len(st.Cols))

	return nil
}

// Generate rows for this table based on the column definitions added to the
// table.
func (st *STTable) GenRows() {
	// GenRows() must be called only for dynamic tables.
	log.Assert(st.IsDynamic, st.Name)

	// Columns must be added before adding rows.
	log.Assert(len(st.Cols) > 0)

	// Generate brand new rows everytime GenRows() is called.
	st.Rows = nil
	st.Width = 0

	// Generate header if not already present.
	if st.Header.Cells == nil {
		// Generate and add the header row.
		headerCells := make([]STCell, len(st.Cols))
		for col := range st.Cols {
			headerCells[col] = STCell{
				Content: st.Cols[col].Header,
				Width:   st.Cols[col].Width,
			}
		}
		st.AddHeader(headerCells)
	}

	keyCol := st.Cols[0]
	// First column must be the key column.
	log.Assert(keyCol.IsKey, st.Cols)

	// Key columns name is the top level data directory that contains subdirs
	// for each row entity. The subdirs in turn contain a directory for each
	// column where that directory has the data files for that column.
	tldd := filepath.Join(DataDir, keyCol.Source)

	entries, err := os.ReadDir(tldd)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// Each sub-directory in entries corresponds to one row in the table.
	for _, entry := range entries {
		info, _ := entry.Info()
		keyName := info.Name()
		row := make([]string, len(st.Cols))

		// Each sub-directory has a directory for each column, except the key
		// column which is the sub-directory name itself.
		for idx, col := range st.Cols {
			if col.IsKey {
				row[idx] = keyName
				continue
			}

			val, err := GetLatestValue(filepath.Join(tldd, keyName, col.Source))
			if err != nil {
				log.Fatalf("failed to get latest value for key: %s, column: %s: %v",
					keyName, col.Header, err)
			}
			row[idx] = val
		}

		st.AddRow(row)
	}
}

// From the given column dir return the latest entry.
func GetLatestValue(colDir string) (string, error) {
	latest := filepath.Join(colDir, "latest")

	data, err := os.ReadFile(latest)
	if err != nil {
		return "", fmt.Errorf("failed to read latest file %s: %v", latest, err)
	}

	return string(data), nil
}

func init() {
	stormgoDir := common.GetStormgoDir()
	ColumnDefinitionsDir = filepath.Join(stormgoDir, "terminal", "columns")
	DataDir = filepath.Join(stormgoDir, "data")
}
