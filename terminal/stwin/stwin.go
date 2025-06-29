package stwin

import (
	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
	"github.com/stormgo/terminal/sttable"
)

type DrawFunc func(*STWin)

// This is the ST window.
// It is a wrapper around goncurses window and can be used for following:
//  1. Display a table with a header and multiple rows.
//     STWin.Table must be valid.
//  2. Display a custom content using a Draw function.
//     STWin.Draw must be set.
//  3. Display an ncurses pad.
//     STWin.Pad must be set.
//     STWin.Table must be nil and STWin.Draw must be set to DrawPad.
type STWin struct {
	//
	// Table to be displayed in this window.
	// See Draw().
	//
	Table *sttable.STTable

	//
	// Draw function to be called to draw the window content.
	// If this is set, Table must not be set, and the Populate() function will
	// simply call this Draw function to draw the content of the window.
	//
	Draw DrawFunc

	//
	// IsHelp is true if this window is a help window.
	// Need this as golang doesn't support comparing function pointers.
	//
	IsHelp bool

	//
	// goncurses window, and it's height, width and starting position (top left corner).
	// The Window content cannot be assumed to be populated and Populate() must
	// be called to populate the Window with content.
	//
	Window *gc.Window
	H      int // Height of the window, including the borders.
	W      int // Width of the window, including the borders.
	X      int
	Y      int

	//
	// Pad is used to store content larger than what can fit in the terminal.
	// DrawPad() then copies the visible content to STWin.Window which is then
	// displayed.
	//
	Pad *gc.Pad
	PH  int // Pad height.
	PW  int // Pad width.
	PX  int // Current starting X coordinate.
	PY  int // Current starting Y coordinate.
}

func NewWin(table *sttable.STTable, y, x int) *STWin {
	// Table must have been properly initialized.
	log.Assert(table.Width > 0)
	log.Assert(table.Name != "")

	log.Assert(y >= 0 && x >= 0, y, x)
	log.Assert(y < stlib.GetMaxRows() && x < stlib.GetMaxCols(),
		y, x, stlib.GetMaxRows(), stlib.GetMaxCols())

	// 1 column each for left and right border.
	w := table.Width + 2

	// Not greater than max columns supported by terminal.
	if x+w > stlib.GetMaxCols() {
		log.Warnf("Window right edge (%d) exceeds max cols (%d), trimming",
			x+w, stlib.GetMaxCols())
		w = stlib.GetMaxCols() - x
	}

	// 1 for the heading and 1 each for the boundary lines on both sides.
	h := table.GetRowCount() + 3
	if y+h > stlib.GetMaxRows() {
		log.Warnf("Window bottom edge (%d) exceeds max rows (%d), trimming",
			y+h, stlib.GetMaxRows())
		h = stlib.GetMaxRows() - y
	}

	return &STWin{
		Table: table,
		H:     h,
		W:     w,
		Y:     y,
		X:     x,
	}
}

// NewDrawWin creates a new "draw window" with the given draw function.
// "Draw Windows" are special type of windows which provide their own draw
// function, they do not have a table associated with them and hence the
// regular Populate() method cannot be used to draw them.
//
// 'h' and 'w' are the height and width of the window excluding the borders.
func NewDrawWin(draw DrawFunc, w, h, y, x int) *STWin {
	log.Assert(draw != nil, "Draw function must not be nil")
	log.Assert(y >= 0 && x >= 0, y, x)
	log.Assert(w >= 0 && h >= 0, w, h)

	//
	// Zero value means use full terminal size in that dimension.
	// Subtract 2 for the borders.
	//
	if w == 0 {
		w = stlib.GetMaxCols() - 2
	}

	// Not greater than max columns supported by terminal.
	if x+w+2 > stlib.GetMaxCols() {
		log.Warnf("Window right edge (%d) exceeds max cols (%d), trimming",
			x+w+2, stlib.GetMaxCols())
		w = stlib.GetMaxCols() - x - 2
	}

	if h == 0 {
		h = stlib.GetMaxRows() - 2
	}

	// Not greater than max rows supported by terminal.
	if y+h+2 > stlib.GetMaxRows() {
		log.Warnf("Window bottom edge (%d) exceeds max rows (%d), trimming",
			y+h+2, stlib.GetMaxRows())
		h = stlib.GetMaxRows() - y - 2
	}

	return &STWin{
		Draw: draw,
		H:    h + 2, // 1 for top border, 1 for bottom border.
		W:    w + 2, // 1 for left border, 1 for right border.
		Y:    y,
		X:    x,
	}
}

// pw and ph are the width and height of the pad. These must be sufficient for
// holding the contents of the pad. If ph is 0 it is set to a default value of
// 2000, which is a large enough value for most use cases. Same for pw. Later,
// when the actual content is added to the pad, the height and width of the
// pad are automatically adjusted to fit the content.
//
// sw and sh are the width and height of the scrollable area excluding the
// borders. If sw or sh is 0, it is set to the maximum terminal size in that
// dimension.
// sy and sx are the starting y and x coordinates of the scrollable area,
// including the border.
func NewPad(draw DrawFunc, ph, pw, sh, sw, sy, sx int) *STWin {
	log.Assert(draw != nil, "Draw function must not be nil")
	log.Assert(sy >= 0 && sx >= 0, sy, sx)
	log.Assert(sw >= 0 && sh >= 0, sw, sh)
	//
	// Zero value means use full terminal size in that dimension.
	// Subtract 2 for the borders.
	//
	if sw == 0 {
		sw = stlib.GetMaxCols() - 2
	}

	// Not greater than max columns supported by terminal.
	if sx+sw+2 > stlib.GetMaxCols() {
		log.Warnf("Window right edge (%d) exceeds max cols (%d), trimming",
			sx+sw+2, stlib.GetMaxCols())
		sw = stlib.GetMaxCols() - sx - 2
	}

	if sh == 0 {
		sh = stlib.GetMaxRows() - 2
	}

	// Not greater than max rows supported by terminal.
	if sy+sh+2 > stlib.GetMaxRows() {
		log.Warnf("Window bottom edge (%d) exceeds max rows (%d), trimming",
			sy+sh+2, stlib.GetMaxRows())
		sh = stlib.GetMaxRows() - sy - 2
	}

	const defaultPadHeight = 2000
	const defaultPadWidth = 2000

	if ph == 0 {
		ph = defaultPadHeight
	}

	if pw == 0 {
		pw = defaultPadWidth
	}

	log.Assert(ph > 0 && pw > 0, ph, pw)

	pad, err := gc.NewPad(ph, pw)
	if err != nil {
		log.Fatalf("gc.NewPad(ph=%d, pw=%d) failed: %v", ph, pw, err)
	}

	return &STWin{
		Draw: draw,
		Pad:  pad,
		H:    sh + 2, // 1 for top border, 1 for bottom border.
		W:    sw + 2, // 1 for left border, 1 for right border.
		Y:    sy,
		X:    sx,
		PH:   ph,
		PW:   pw,
	}
}

func (sw *STWin) IsHelpWindow() bool {
	return sw.IsHelp
}

func (sw *STWin) IsPad() bool {
	return sw.Pad != nil
}

func (sw *STWin) GetName() string {
	if sw.IsHelpWindow() {
		return "Help Window"
	} else if sw.Table != nil {
		return sw.Table.Name
	} else {
		// TODO: Need a specific name for the draw window.
		return "Draw Window"
	}
}

// Does the given (y, x) coordinate fall within this window?
func (sw *STWin) FallsInWindow(y, x int) bool {
	log.Assert(y >= 0 && x >= 0, y, x)
	// Impossibly high values.
	log.Assert(y <= stlib.GetMaxRows() && x <= stlib.GetMaxCols(),
		y, x, stlib.GetMaxRows(), stlib.GetMaxCols())

	return y >= sw.Y && y < sw.Y+sw.H && x >= sw.X && x < sw.X+sw.W
}

// Does the given (y, x) coordinate fall within any column header?
// Returns the column index if it falls in a column header, or -1 if it does
// not.
func (sw *STWin) FallsInColumnHeader(y, x int) int {
	log.Assert(y >= 0 && x >= 0, y, x)
	log.Assert(y <= stlib.GetMaxRows() && x <= stlib.GetMaxCols(), y, x)

	//
	// Click doesn't fall in the header row.
	// sw.Y is the border followed immediately by the header row.
	//
	if y != sw.Y+1 {
		return -1
	}

	// First column header starts at sw.X + 1 (1 for the left border).
	startCol := sw.X + 1
	endCol := 0

	hdr := sw.Table.Header.Cells
	for i, cell := range hdr {
		endCol = startCol + cell.Width

		if x >= startCol && x < endCol {
			return i
		}

		// Next column starts after current column, leaving one space.
		startCol = endCol + 1
	}

	return -1
}

// Populate the window with the table contents.
func (sw *STWin) Populate(inFocus bool) {
	//stlib.PrintStatus("Populating window %s (Y=%d, X=%d, H=%d, W=%d)",
	//	sw.Table.Name, sw.Y, sw.X, sw.H, sw.W)

	//
	// Everytime we create a new gocurses window and populate it afresh.
	//
	win, err := gc.NewWindow(sw.H, sw.W, sw.Y, sw.X)
	if err != nil {
		log.Fatalf("gc.NewWindow(H=%d, W=%d, Y=%d, X=%d), name: %s, failed: %v",
			sw.H, sw.W, sw.Y, sw.X, sw.Table.Name, err)
	}

	if sw.Window != nil {
		// Free up the memory allocated for the previous window by cgo.
		err := sw.Window.Delete()
		log.Assert(err == nil, err)
	}

	sw.Window = win

	//
	// If Draw function is set, call it to draw the window content.
	// In case of a Pad, the Draw function populates the Pad and not the
	// Window. DrawPad() then copies the currently visible content from the
	// Pad to goncurses Window.
	//
	if sw.Draw != nil {
		// Table must not be set when Draw function is set.
		log.Assert(sw.Table == nil, sw.GetName())

		sw.Draw(sw)

		if sw.IsPad() {
			// Copy the visible content from the Pad to the Window.
			DrawPad(sw)
		}

		return
	}

	// Pad must have Draw function set.
	log.Assert(!sw.IsPad(), sw.GetName())

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
	win.ColorOn(stlib.BlackOnCyan)
	for _, cell := range sw.Table.Header.Cells {
		paddedStr := common.TruncateAndPadUTF8String(cell.Content, cell.Width)
		win.MovePrint(y, x, paddedStr)

		if cell.Sort == sttable.SortOrderAsc {
			win.MoveAddWChar(y, x+cell.Width, '△')
		} else if cell.Sort == sttable.SortOrderDesc {
			win.MoveAddWChar(y, x+cell.Width, '▽')
		}

		x += (cell.Width + 1)
	}
	win.ColorOff(stlib.BlackOnCyan)

	//
	// Print all rows.
	//
	y = 2
	for _, row := range sw.Table.Rows {
		x = 1
		for _, cell := range row.Cells {
			// Data rows must not have sort order set.
			log.Assert(cell.Sort == sttable.SortOrderNone, cell.Sort)

			paddedStr := common.TruncateAndPadUTF8String(cell.Content, cell.Width)
			win.MovePrint(y, x, paddedStr)
			x += (cell.Width + 1)
		}
		y++
	}
}

// Called when 'key' is pressed while this window is in focus.
func (sw *STWin) HandleKey(key gc.Key) {
	stlib.PrintStatus("Key %v pressed in window '%s'",
		gc.KeyString(key), sw.GetName())

	//
	// Handle scrolling for the pad.
	//
	if sw.IsPad() {
		PadHandleKey(sw, key)
	}
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
	colIdx := sw.FallsInColumnHeader(y, x)

	stlib.PrintStatus("Mouse clicked at (%d, %d) in window %s (col: %d)",
		y, x, sw.Table.Name, colIdx)

	// If the click does not fall in a column header, do nothing.
	if colIdx < 0 {
		return
	}

	// else, sort the table by the clicked column.
	cell := &sw.Table.Header.Cells[colIdx]

	// Toggle the sort order for the clicked column.
	if cell.Sort == sttable.SortOrderNone || cell.Sort == sttable.SortOrderDesc {
		cell.Sort = sttable.SortOrderAsc
	} else {
		cell.Sort = sttable.SortOrderDesc
	}

	// Only one column can be sorted at a time.
	for i := range sw.Table.Header.Cells {
		if i != colIdx {
			sw.Table.Header.Cells[i].Sort = sttable.SortOrderNone
		}
	}

	sw.Table.Sort(colIdx, cell.Sort)
}
