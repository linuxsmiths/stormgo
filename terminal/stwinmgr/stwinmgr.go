package stwinmgr

import (
	"math"
	"slices"
	"sync"

	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
	"github.com/stormgo/terminal/stwin"
)

var (
	winmgr             *STWinMgr                      // Singleton Window manager instance.
	refreshIntervalSec = 2                            // Refresh interval in seconds.
	keyChan            = make(chan gc.Key, 1)         // Channel for input handling.
	mouseChan          = make(chan *gc.MouseEvent, 1) // Channel for mouse events.
	done               = make(chan bool)              // Channel to signal when the program is done.
	wg                 sync.WaitGroup                 // WaitGroup to wait for goroutines to finish.
)

// This is the ST window manager. It's a singleton class since we can have
// only one window manager at a time in the terminal. All the functions in
// this package operate on this singleton instance, winmgr.
//
// It contains and manages all the windows created (may or may not be currently
// displayed). It assumes it has access to the entire screen and can place
// windows anywhere on the screen, obviously according to their coordinates.
type STWinMgr struct {
	//
	// Window manager manages multiple windows, each which is represented by
	// stwin.STWin object that has the STTable object which contains the
	// data to be displayed, along with the goncurses window object and it's
	// size and location. This information is sufficient to paint the windows.
	// Window at index 0 is the topmost window, followed by the next window at
	// index 1, and so on. Only the topmost window is in focus at any given
	// time. This is the window that gets to process the key presses.
	// A window can be brought in focus (top of the windows stack) by clicking
	// on it with the mouse anywhere in the window, or by using the Tab key to
	// cycle through all the windows.
	// Note that while there can be only one in-focus window, multiple windows
	// can be visible (fully or partially) depending on their size and
	// location. If the in-focus window doesn't cover another window entirely,
	// that other window will still be visible and can be brought in focus by
	// clicking on it with the mouse.
	//
	Windows []*stwin.STWin

	//
	// Help window, if user pressed the 'h' key, else nil.
	//
	Help *stwin.STWin

	//
	// Mouse press/release info, for supporting click, and dragging.
	// MousePressWinIdx is the index of the window where the mouse was
	// pressed. -2 means the mouse is not currently pressed, -1 means the
	// mouse was pressed in a place where there was no window.
	//
	MousePressWinIdx int
	MousePressY      int // Y coordinate of the mouse press, -1 if not currently pressed.
	MousePressX      int // X coordinate of the mouse press, -1 if not currently pressed.
	MouseDragX       int // X coordinate of the mouse drag, -1 if not currently dragging.
	MouseDragY       int // Y coordinate of the mouse drag, -1 if not currently dragging.
}

func newWinMgr() *STWinMgr {
	// Must be called only once.
	log.Assert(winmgr == nil)

	return &STWinMgr{
		MousePressWinIdx: -2,
		MousePressY:      -1,
		MousePressX:      -1,
		MouseDragX:       -1,
		MouseDragY:       -1,
	}
}

func infocusWindow() *stwin.STWin {
	// Help window, if present, is always in focus.
	if winmgr.Help != nil {
		return winmgr.Help
	}

	// No windows.
	if len(winmgr.Windows) == 0 {
		return nil
	}

	// Else the topmost window is the in-focus window.
	return winmgr.Windows[0]
}

// Called when user presses 'h' key.
func showHelp() {
	//
	// Create the help window if it doesn't exist.
	// Help window is a "draw window" i.e., it has its own draw function and
	// doesn't have a table associated with it. Additionally it's a Pad i.e.,
	// it can be larger than the terminal window and can be scrolled.
	//
	if winmgr.Help == nil {
		winmgr.Help = stwin.NewPad(stwin.DrawHelp, 0, 0, 0, 0, 0, 0)
		winmgr.Help.IsHelp = true
		// Help window should not have a table associated with it.
		log.Assert(winmgr.Help.Table == nil)

		//
		// Clear the screen only the first time when help window is shown, not
		// on every refresh, to avoid flicker.
		//
		stlib.ClearScreen()
	}

	//
	// Populate() will call the DrawHelp() function to fill the Pad and call
	// DrawPad() to draw the visible part of the Pad on the terminal.
	//
	winmgr.Help.Populate(true)
	winmgr.Help.Window.Refresh()
}

func closeHelp() {
	winmgr.Help = nil
	stlib.ClearScreen()
}

// Refresh and redraw all the windows managed by the window manager.
// This function iterates over all the windows starting from the lowest in
// the stack and moving upwards, calls their Populate() method to fill them
// with data, and then paints them using panels for proper stacking and
// visibility.
func refresh() {
	if winmgr.Help != nil {
		showHelp()
		return
	}

	panels := make([]*gc.Panel, len(winmgr.Windows))

	for i := len(winmgr.Windows) - 1; i >= 0; i-- {
		win := winmgr.Windows[i]
		//stlib.PrintStatus("Populating window[H=%d, W=%d, Y=%d, X=%d] %d/%d <%s>",
		//	win.H, win.W, win.Y, win.X, i+1, len(winmgr.Windows), win.Table.Name)

		// Populate the window with data from the STTable.
		win.Populate(i == 0 /* inFocus */)

		//
		// win can be a window or a pad.
		// We care about the underlying goncurses window, which we will need
		// to pass to gc.NewPanel() to create a panel for this window.
		// Whatever content is there on underlying ncurses window, it will be
		// painted by the panel update.
		//
		ncwin := win.Window

		// Populate() must set win.Window to a valid goncurses window.
		log.Assert(ncwin != nil)

		// Create a new panel and place it at the top of the stack.
		panels[i] = gc.NewPanel(ncwin)
	}

	gc.UpdatePanels()
	gc.Update()

	for i := 0; i < len(panels); i++ {
		err := panels[i].Delete()
		log.Assert(err == nil, "Failed to delete panel: %v", err)
	}
}

func AddWindow(win *stwin.STWin) {
	// goncurses must have been initialized.
	log.Assert(stlib.Stdscr != nil)
	log.Assert(win != nil)
	// Same window must not be added more than once.
	log.Assert(!slices.Contains(winmgr.Windows, win), win.Table.Name)

	// Adds the window to the top of the stack.
	winmgr.Windows = append(winmgr.Windows, win)
}

// Call this to refocus the top window when the user presses the Tab key.
// This will shift the top window to the bottom of the list, and move every
// other window one level up the stack.
func RefocusOnTabPress() {
	// No windows or just one window, nothing to do.
	if len(winmgr.Windows) <= 1 {
		return
	}

	//
	// Perform the left/top shift.
	// This will take the first (topmost) window in the list and move it to the
	// end (bottom) of the stack and moves every other window one level left/up.
	//
	topWindow := winmgr.Windows[0]
	winmgr.Windows = append(winmgr.Windows[1:], topWindow)

	refresh()
}

// Call this to refocus a window when the user clicks on it with the mouse.
// This will take the window at the given index and move it to the top of the
// stack, making it the in-focus window. Every other window which were above
// this window is shifted one level down the stack.
func RefocusOnMouseClick(y, x int) {
	// Find the window that was clicked based on the mouse coordinates (y, x).
	idx := getMouseClickWindow(y, x)

	stlib.PrintStatus("Refocusing on mouse click at (%d, %d), window index: %d", y, x, idx)

	//
	// No window was clicked or the clicked window is already at the top.
	// If infocus window is clicked we pass the coordinates to it so it can
	// perform actions like sorting based on the clicked column header.
	//
	if idx <= 0 {
		if idx == 0 {
			win := infocusWindow()
			win.HandleMouse(y, x)
		}
		return
	}

	clickedWindow := winmgr.Windows[idx]

	for i := idx; i > 0; i-- {
		winmgr.Windows[i] = winmgr.Windows[i-1]
	}

	winmgr.Windows[0] = clickedWindow

	refresh()
}

// Given the mouse coordinates (y, x) return the index of the window that was
// clicked.
// In case of overlapping windows, the topmost window that contains the mouse
// click will be returned.
// If no window lies on the clicked coordinates, returns -1.
func getMouseClickWindow(y, x int) int {
	//
	// Starting from the top of the stack, find the first window that contains
	// the clicked coordinates (y, x). This is the window that was clicked and
	// must have the focus.
	//
	idx := -1
	for i, win := range winmgr.Windows {
		if win.FallsInWindow(y, x) {
			stlib.PrintStatus("Window %d/%d [%s] clicked at (%d, %d)",
				i, len(winmgr.Windows), win.Table.Name, y, x)
			idx = i
			break
		}
	}

	return idx
}

func HandleMouseLeftButtonPress(y, x int) {
	//
	// An already pressed mouse cannot be pressed again without releasing it
	// first.
	//
	log.Assert(winmgr.MousePressWinIdx == -2, winmgr.MousePressWinIdx)
	log.Assert(winmgr.MousePressY == -1, winmgr.MousePressY)
	log.Assert(winmgr.MousePressX == -1, winmgr.MousePressX)

	// Find the window that was clicked based on the mouse coordinates (y, x).
	idx := getMouseClickWindow(y, x)
	winmgr.MousePressWinIdx = idx

	// Note the x and y coordinates of the mouse press.
	winmgr.MousePressY = y
	winmgr.MousePressX = x

	// Mouse clicked outside all windows, nothing to do.
	if idx == -1 {
		stlib.PrintStatus("Left mouse button press at (%d, %d), over no window", y, x)
		return
	}

	clickedWindow := winmgr.Windows[idx]
	stlib.PrintStatus("Left mouse button press at (%d, %d), window: %s (index: %d)",
		y, x, clickedWindow.Table.Name, idx)
}

func HandleMouseLeftButtonRelease(y, x int) {
	//
	// Only a pressed mouse button can be released.
	//
	log.Assert(winmgr.MousePressWinIdx != -2, winmgr.MousePressWinIdx)
	log.Assert(winmgr.MousePressY != -1, winmgr.MousePressY)
	log.Assert(winmgr.MousePressX != -1, winmgr.MousePressX)

	idx := winmgr.MousePressWinIdx

	//
	// Button no longer pressed.
	// Reset these values, but just before returning, since we need to use
	// these values to determine if this is a click or a drag and by how much
	// did the mouse move.
	//
	defer func() {
		winmgr.MousePressWinIdx = -2
		winmgr.MousePressY = -1
		winmgr.MousePressX = -1
		winmgr.MouseDragY = -1
		winmgr.MouseDragX = -1
	}()

	if idx == -1 {
		stlib.PrintStatus("Left mouse button released at (%d, %d), pressed over no window",
			y, x)
		return
	}

	clickedWindow := winmgr.Windows[idx]

	deltaY := y - winmgr.MousePressY
	deltaX := x - winmgr.MousePressX

	//
	// If the mouse was clicked and released at the same position, it implies
	// a click event, not a drag.
	//
	if deltaY == 0 && deltaX == 0 {
		stlib.PrintStatus("Left mouse button clicked at (%d, %d), window: %s (index: %d)",
			y, x, clickedWindow.Table.Name, idx)
		RefocusOnMouseClick(y, x)
		return
	}

	//
	// If it's not a click, but a drag, we must have updated drag coordinates.
	//
	log.Assert(winmgr.MouseDragY != -1 && winmgr.MouseDragX != -1,
		winmgr.MouseDragY, winmgr.MouseDragX)

	//
	// We don't update on release, rather we must have updated on mouse
	// move/drag events.
	//
	stlib.PrintStatus("Left mouse button dragged and released at (%d[%d], %d[%d]), window: %s (index: %d)",
		y, deltaY, x, deltaX, clickedWindow.Table.Name, idx)

	deltaY = int(math.Abs(float64(y - winmgr.MouseDragY)))
	deltaX = int(math.Abs(float64(x - winmgr.MouseDragX)))

	//
	// We must have updated drag coordinates in HandleMouseLeftButtonDrag() so
	// in HandleMouseLeftButtonRelease() we must not see too big a difference.
	//
	log.Assert(deltaY <= 2, deltaY)
	log.Assert(deltaX <= 2, deltaX)
}

func HandleMouseLeftButtonDrag(y, x int) {
	//
	// Dragging without pressing, don't do anything.
	//
	if winmgr.MousePressWinIdx == -2 {
		stlib.PrintStatus("Left mouse button drag (%d, %d), w/o button press",
			y, x)
		return
	}

	log.Assert(winmgr.MousePressY != -1, winmgr.MousePressY)
	log.Assert(winmgr.MousePressX != -1, winmgr.MousePressX)

	idx := winmgr.MousePressWinIdx

	// Drag while clicked over no window, nothing to do.
	if idx == -1 {
		stlib.PrintStatus("Left mouse button drag (%d, %d), button pressed over no window",
			y, x)
		return
	}

	clickedWindow := winmgr.Windows[idx]
	stlib.PrintStatus("Left mouse button drag (%d, %d) button pressed over window: %s (index: %d)",
		y, x, clickedWindow.Table.Name, idx)

	deltaX := 0
	deltaY := 0

	// Either none of the drag coordinates are set, or both are set.
	log.Assert((winmgr.MouseDragY == -1 && winmgr.MouseDragX == -1) ||
		(winmgr.MouseDragY != -1 && winmgr.MouseDragX != -1),
		winmgr.MouseDragX, winmgr.MouseDragY)

	//
	// if this is the first drag event after the click, take delta from the
	// clicked position, else take delta from the last drag position.
	//
	if winmgr.MouseDragY == -1 || winmgr.MouseDragX == -1 {
		deltaY = y - winmgr.MousePressY
		deltaX = x - winmgr.MousePressX
	} else {
		deltaY = y - winmgr.MouseDragY
		deltaX = x - winmgr.MouseDragX
	}

	clickedWindow.Y += deltaY
	clickedWindow.X += deltaX

	if clickedWindow.Y < 0 {
		clickedWindow.Y = 0
	} else if clickedWindow.Y+clickedWindow.H >= stlib.GetMaxRows() {
		clickedWindow.Y = stlib.GetMaxRows() - clickedWindow.H
	}

	if clickedWindow.X < 0 {
		clickedWindow.X = 0
	} else if clickedWindow.X+clickedWindow.W >= stlib.GetMaxCols() {
		clickedWindow.X = stlib.GetMaxCols() - clickedWindow.W
	}

	winmgr.MouseDragY = y
	winmgr.MouseDragX = x
}

// This function runs indefinitely, processing user input and refreshing the
// windows periodically.
// This MUST be called by one one thread, and that thread is solely
// responsible for managing the terminal, handling user input and refreshing
// the windows.
func Run() {
	// Refresh once at start.
	refresh()

	//
	// GetChar() should come out every refreshIntervalSec, so that we can
	// refresh the windows periodically.
	//
	stlib.StatusWindow.Timeout(refreshIntervalSec * 1000)

	for {
		ch := stlib.StatusWindow.GetChar()

		if ch != 0 {
			stlib.PrintStatus("Got key: %s", gc.KeyString(ch))
			if ch == gc.KEY_MOUSE {
				mevt := gc.GetMouse()
				//
				// TODO: See why we are getting nil mouse events sometimes.
				//
				if mevt != nil {
					stlib.PrintStatus("Got mouse event: %+v", *mevt)
					HandleMouse(mevt)
				}
			} else {
				HandleInput(ch)
			}
		}

		// Refresh all the windows periodically or after a key/mouse event.
		refresh()
	}
}

func HandleInput(ch gc.Key) {
	switch ch {
	case 'h':
		showHelp()
	case 'q':
		win := infocusWindow()
		if win != nil {
			// Quit on a help window? Close the help window.
			if win.IsHelpWindow() {
				closeHelp()
			} else {
				win.HandleQuit()
			}
		} else {
			stlib.EndTerminal()
		}
	case gc.KEY_TAB:
		RefocusOnTabPress()
	case gc.KEY_MOUSE:
		// HandleMouse() must be called for mouse click events.
		log.Assert(false)
	default:
		win := infocusWindow()
		if win != nil {
			win.HandleKey(ch)
		}
	}
}

func HandleMouse(mevt *gc.MouseEvent) {
	log.Assert(mevt != nil)
	log.Assert(mevt.X >= 0 && mevt.X <= stlib.GetMaxCols(), mevt.X, stlib.GetMaxCols())
	// +1 as GetMaxRows() returns one less row.
	log.Assert(mevt.Y >= 0 && mevt.Y <= stlib.GetMaxRows()+1, mevt.Y, stlib.GetMaxRows())

	stlib.PrintStatus("Mouse event: %+v", *mevt)

	// Left mouse button click.
	if mevt.State&gc.M_B1_CLICKED != 0 {
		RefocusOnMouseClick(mevt.Y, mevt.X)
	} else if mevt.State&gc.M_B1_PRESSED != 0 {
		HandleMouseLeftButtonPress(mevt.Y, mevt.X)
	} else if mevt.State&gc.M_B1_RELEASED != 0 {
		HandleMouseLeftButtonRelease(mevt.Y, mevt.X)
	} else if mevt.State&gc.M_POSITION != 0 {
		HandleMouseLeftButtonDrag(mevt.Y, mevt.X)
	}
}

func Start() {
	winmgr = newWinMgr()
}

func End() {
	done <- true
}
