package stwinmgr

import (
	"sync"
	"time"

	gc "github.com/gbin/goncurses"
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
}

func newWinMgr() *STWinMgr {
	// Must be called only once.
	log.Assert(winmgr == nil)

	return &STWinMgr{}
}

func infocusWindow() *stwin.STWin {
	// No windows.
	if len(winmgr.Windows) == 0 {
		return nil
	}

	// Else the topmost window is the in-focus window.
	return winmgr.Windows[0]
}

// Refresh and redraw all the windows managed by the window manager.
// This function iterates over all the windows starting from the lowest in
// the stack and moving upwards, calls their Populate() method to fill them
// with data, and then paints them using panels for proper stacking and
// visibility.
func refresh() {
	for i := len(winmgr.Windows) - 1; i >= 0; i-- {
		win := winmgr.Windows[i]
		stlib.PrintStatus("Populating window %d/%d <%s>",
			i+1, len(winmgr.Windows), win.Table.Name)

		// Populate the window with data from the STTable.
		win.Populate(i == 0 /* inFocus */)
		// Create a new panel and place it at the top of the stack.
		gc.NewPanel(win.Window)
	}

	gc.UpdatePanels()
	gc.Update()
}

func AddWindow(win *stwin.STWin) {
	// goncurses must have been initialized.
	log.Assert(stlib.Stdscr != nil)
	log.Assert(win != nil)

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

// This function runs indefinitely, processing user input and refreshing the
// windows periodically.
// This MUST be called by one one thread, and that thread is solely
// responsible for managing the terminal, handling user input and refreshing
// the windows.
func Run() {
	// Refresh once at start.
	refresh()

	for {
		select {
		case ch := <-keyChan:
			HandleInput(ch)
		case mevt := <-mouseChan:
			HandleMouse(mevt)
		case <-time.After(time.Duration(refreshIntervalSec) * time.Second):
			refresh()
		}
	}
}

func HandleInput(ch gc.Key) {
	switch ch {
	case 'q':
		win := infocusWindow()
		if win != nil {
			win.HandleQuit()
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
	RefocusOnMouseClick(mevt.Y, mevt.X)
}

func Start() {
	winmgr = newWinMgr()
	//
	// Spawn a goroutine to gather user input (keyboard or mouse) and pass it
	// on to the runner goroutine, over the input channel.
	// This should be the only goroutine that reads the terminal/mouse events.
	//
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			ch := stlib.Stdscr.GetChar()

			stlib.PrintStatus("Queueing input: %s", gc.KeyString(ch))
			if ch == gc.KEY_MOUSE {
				mevt := gc.GetMouse()
				//
				// TODO: See why we are getting nil mouse events sometimes.
				//
				if mevt != nil {
					stlib.PrintStatus("Got mouse event: %+v", *mevt)
					mouseChan <- mevt
				}
			} else {
				keyChan <- ch
			}
		}
	}()
}

func End() {
	done <- true
}
