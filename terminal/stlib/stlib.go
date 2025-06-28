package stlib

//
// stlib is a library of commonly used functions and types, to be used by the
// terminal program. This is the most fundamental package, to be imported by
// other terminal packages, this MUST NOT import any terminal package.
//

import (
	"fmt"
	"os"
	"unicode/utf8"

	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common/log"
)

var (
	//
	// The root window of the terminal.
	// We use it only to find terminal size and clearing the screen.
	// When this is refreshed, either explicitly or implicitly by GetChar(),
	// it overlays the stdscr on the screen which is not what we want, hence
	// we simply stay away from this.
	//
	Stdscr *gc.Window

	//
	// One line status window at the bottom of the terminal.
	// To avoid unwanted updates to screen by stdscr.GetChar() implicitly
	// refreshing the terminal, we use StatusWindow for reading input too.
	//
	StatusWindow *gc.Window
)

// Color pairs used in the terminal.
const (
	// 0 is reserved for the default color pair.
	Invalid      int16 = iota
	BlackOnBlack       // Only useful for setting background to black.
	WhiteOnWhite       // Only useful for setting background to white.
	BlackOnWhite
	WhiteOnBlack
	RedOnBlack
	GreenOnBlack
	CyanOnBlack
	YellowOnBlack
	BlackOnCyan
)

func testUnicode() {
	maxY, maxX := Stdscr.MaxYX()
	PrintStatus("Testing unicode characters on %dx%d terminal, <ENTER> to start, 'q' to quit",
		maxX, maxY)

	key := Stdscr.GetChar()
	if key == 'q' || key == 'Q' {
		return
	}

	codePoint := 0

	for codePoint < 0x10FFFF {
		for y := 0; y < maxY-2; y++ {
			for x := 0; x < maxX; x++ {
				if utf8.ValidRune(rune(codePoint)) {
					Stdscr.MoveAddWChar(y, x, gc.WChar(codePoint))
				}
				codePoint++
			}
		}

		Stdscr.Refresh()

		PrintStatus("<ENTER> for next page, 'q' to quit")
		key = Stdscr.GetChar()
		if key == 'q' || key == 'Q' {
			break
		}
	}
}

// Initialize the ncurses terminal.
func InitTerminal() {
	// Must be called only once.
	log.Assert(Stdscr == nil)

	var err error

	Stdscr, err = gc.Init()
	if err != nil {
		log.Fatalf("Failed to initialize ncurses: %v", err)
	}

	//
	// Require a minimum terminal size of 80x20.
	// TODO: Once we learn more about the usecase, we can make this
	//       configurable, or relax it. Till then avoid surprises.
	//
	minY, minX := 20, 80
	maxY, maxX := Stdscr.MaxYX()
	if maxY < minY || maxX < minX {
		log.Fatalf("Need at least %dx%d terminal size, got %dx%d", minX, minY, maxX, maxY)
	}

	//
	// Setup the status window early on to emit important status messages.
	// It's a one line window at the bottom of the terminal.
	// Now PrintStatus() can be called.
	//
	StatusWindow, err = gc.NewWindow(1, maxX, maxY-1, 0)
	if err != nil {
		log.Fatalf("Failed to create status window: %v", err)
	}

	PrintStatus("ncurses initialized successfully")

	if !gc.HasColors() {
		log.Fatalf("Requires a terminal that supports colors")
	}

	// Must be called after Init but before using any colour related functions
	if err := gc.StartColor(); err != nil {
		log.Fatalf("Failed to start color support: %v", err)
	}

	// Setup various color pairs we are going to use.
	gc.InitPair(BlackOnBlack, gc.C_BLACK, gc.C_BLACK)
	gc.InitPair(WhiteOnWhite, gc.C_WHITE, gc.C_WHITE)
	gc.InitPair(WhiteOnBlack, gc.C_WHITE, gc.C_BLACK)
	gc.InitPair(BlackOnWhite, gc.C_BLACK, gc.C_WHITE)
	gc.InitPair(RedOnBlack, gc.C_RED, gc.C_BLACK)
	gc.InitPair(GreenOnBlack, gc.C_GREEN, gc.C_BLACK)
	gc.InitPair(CyanOnBlack, gc.C_CYAN, gc.C_BLACK)
	gc.InitPair(YellowOnBlack, gc.C_YELLOW, gc.C_BLACK)
	gc.InitPair(BlackOnCyan, gc.C_BLACK, gc.C_CYAN)

	gc.Echo(false)  // Don't echo while we do getch().
	gc.CBreak(true) // Line buffering disabled.
	gc.Cursor(0)    // Hide the cursor.
	//
	// Enable special keys to be captured.
	// Needed for mouse support and function keys.
	// Note that we perform GetChar() on StatusWindow and not Stdscr.
	//
	StatusWindow.Keypad(true)

	if gc.MouseOk() {
		log.Fatalf("Mouse support is not available in this terminal")
	}

	//
	// Enable mouse left button press.
	// This is only mouse button that we will use for refocussing and sorting.
	// For Window resize and dragging we need to enable all mouse events.
	//
	//gc.MouseMask(gc.M_B1_PRESSED, nil)
	gc.MouseMask(gc.M_ALL|gc.M_POSITION, nil)

	//
	// We don't process mouse click event, we only process press and release.
	// Press and release if not interleaved with drag will be treated as a
	// click.
	//
	gc.MouseInterval(0)

	//
	// This terminal sequence enables mouse drag event reporting.
	// https://gist.github.com/sylt/93d3f7b77e7f3a881603
	//
	fmt.Printf("\033[?1003h\n")

	ClearScreen()

	//testUnicode()
	PrintStatus("Terminal initialized successfully")
}

// EndTerminal cleans up the terminal and exits the program.
func EndTerminal() {
	gc.End()
	// Disable mouse drag event reporting.
	fmt.Printf("\033[?1003l\n")
	os.Exit(0)
}

func GetMaxRows() int {
	// Must be called only after ncurses has been initialized.
	log.Assert(Stdscr != nil)

	maxY, _ := Stdscr.MaxYX()

	// We return maxY - 1 because the last row is used for status window.
	return maxY - 1
}

func GetMaxCols() int {
	// Must be called only after ncurses has been initialized.
	log.Assert(Stdscr != nil)

	_, maxX := Stdscr.MaxYX()
	return maxX
}

// Print a status line.
// The status line is printed in the bottom row of the terminal.
func PrintStatus(format string, params ...interface{}) {
	// Must be called only after ncurses has been initialized.
	log.Assert(StatusWindow != nil)
	log.Assert(len(format) > 0)

	StatusWindow.ColorOn(BlackOnCyan)
	if len(params) == 0 {
		s := fmt.Sprintf(">> " + format)
		// Pad with spaces to fill the entire line.
		s = fmt.Sprintf("%-*s", GetMaxCols(), s)
		StatusWindow.MovePrintf(0, 0, s)
	} else {
		s := fmt.Sprintf(">> "+format, params...)
		s = fmt.Sprintf("%-*s", GetMaxCols(), s)
		StatusWindow.MovePrintf(0, 0, s)
	}
	StatusWindow.ColorOff(BlackOnCyan)

	StatusWindow.ClearToEOL()
	StatusWindow.Refresh()
}

func ClearScreen() {
	// Must be called only after ncurses has been initialized.
	log.Assert(Stdscr != nil)
	Stdscr.SetBackground(gc.Char(' ') | gc.ColorPair(BlackOnBlack))
	Stdscr.Clear()
	Stdscr.Refresh()
}
