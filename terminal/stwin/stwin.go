package stwin

import (
	gc "github.com/gbin/goncurses"
	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
	"github.com/stormgo/terminal/sttable"
)

// This is the ST window.
// It displays an STTable.
type STWin struct {
	//
	// Table to be displayed in this window.
	//
	Table *sttable.STTable

	//
	// goncurses window, and it's height, width and starting position (top left corner).
	// The Window content cannot be assumed to be populated and Populate() must
	// be called to populate the Window with content.
	//
	Window *gc.Window
	H      int
	W      int
	X      int
	Y      int
}

func NewWin(table *sttable.STTable, h, y, x int) *STWin {
	// Table must have been properly initialized.
	log.Assert(table.Width > 0)
	log.Assert(table.Name != "")

	return &STWin{
		Table: table,
		H:     h,
		W:     table.Width,
		Y:     y,
		X:     x,
	}
}

// Does the given (y, x) coordinate fall within this window?
func (sw *STWin) FallsInWindow(y, x int) bool {
	log.Assert(y >= 0 && x >= 0, y, x)
	// Impossibly high values.
	log.Assert(y < 1000 && x < 1000, y, x)

	return y >= sw.Y && y < sw.Y+sw.H && x >= sw.X && x < sw.X+sw.W
}

// Populate the window with the table contents.
func (sw *STWin) Populate(inFocus bool) {
	stlib.PrintStatus("Populating window %s (Y=%d, X=%d, H=%d, W=%d)",
		sw.Table.Name, sw.Y, sw.X, sw.H, sw.W)

	//
	// Everytime we create a new gocurses window and populate it afresh.
	//
	win, err := gc.NewWindow(sw.H, sw.W, sw.Y, sw.X)
	if err != nil {
		log.Fatalf("gc.NewWindow failed: %v", err)
	}

	//
	// Draw a box around the window.
	// If the window is in focus, we draw it with a different color.
	//
	if inFocus {
		win.ColorOn(stlib.GreenOnBlack)
		err = win.Box(0, 0)
		win.ColorOff(stlib.GreenOnBlack)
	} else {
		err = win.Box(0, 0)
	}
	log.Assert(err == nil, err)

	//
	// Print the title bar.
	//
	win.MovePrint(0, 1, sw.Table.Name)

	//
	// Print the header.
	//
	y := 1
	x := 1
	for _, cell := range sw.Table.Header.Cells {
		win.MovePrint(y, x, common.TruncateUTF8String(cell.Content, cell.Width))
		x += (cell.Width + 1)
	}

	//
	// Print all rows.
	//
	y = 2
	for _, row := range sw.Table.Rows {
		x = 1
		for _, cell := range row.Cells {
			win.MovePrint(y, x, common.TruncateUTF8String(cell.Content, cell.Width))
			x += (cell.Width + 1)
		}
		y++
	}

	sw.Window = win
}

// Called when 'key' is pressed while this window is in focus.
func (sw *STWin) HandleKey(key gc.Key) {
	// For now, we just log the key pressed.
	stlib.PrintStatus("Key %v pressed in window %s", gc.KeyString(key), sw.Table.Name)
}

// Called when user presses the quit character (usually 'q') when this window
// is in focus.
// Quit can either close this window (if it's a temporary window created for
// performing some operation) or quit the entire application.
func (sw *STWin) HandleQuit() {
	// For now, we always quit the application.
	stlib.PrintStatus("Quit pressed in window %s", sw.Table.Name)
	stlib.EndTerminal()
}

// Called when mouse left key is pressed while this window is in focus.
func (sw *STWin) HandleMouse(y, x int) {
	// For now, we just log the coordinates clicked.
	stlib.PrintStatus("Mouse clicked at (%d, %d) in window %s", y, x, sw.Table.Name)
}
