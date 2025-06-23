package stlib

//
// stlib is a library of commonly used functions and types, to be used by the
// terminal program. This is the most fundamental package, to be imported by
// other terminal packages, this MUST NOT import any terminal package.
//

import (
	"os"
	"unicode/utf8"

	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common/log"
)

var (
	// The root window of the terminal.
	Stdscr *gc.Window
)

// Color pairs used in the terminal.
const (
	// 0 is reserved for the default color pair.
	Invalid int16 = iota
	RedOnBlack
	GreenOnBlack
	CyanOnBlack
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

	PrintStatus("ncurses initialized successfully")

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

	if !gc.HasColors() {
		log.Fatalf("Requires a terminal that supports colors")
	}

	gc.Echo(false)  // Don't echo while we do getch().
	gc.CBreak(true) // Line buffering disabled.
	gc.Cursor(0)    // Hide the cursor.
	//
	// Enable special keys to be captured.
	// Needed for mouse support and function keys.
	//
	Stdscr.Keypad(true)

	// Must be called after Init but before using any colour related functions
	if err := gc.StartColor(); err != nil {
		log.Fatalf("Failed to start color support: %v", err)
	}

	if gc.MouseOk() {
		log.Fatalf("Mouse support is not available in this terminal")
	}

	//
	// Enable mouse left button press.
	// This is only mouse button that we will use for refocussing and sorting.
	//
	gc.MouseMask(gc.M_B1_PRESSED, nil)

	// Adjust the default mouse-click sensitivity to make it more responsive
	gc.MouseInterval(50)

	// Setup various color pairs we are going to use.
	gc.InitPair(RedOnBlack, gc.C_RED, gc.C_BLACK)
	gc.InitPair(GreenOnBlack, gc.C_GREEN, gc.C_BLACK)
	gc.InitPair(CyanOnBlack, gc.C_CYAN, gc.C_BLACK)
	gc.InitPair(BlackOnCyan, gc.C_BLACK, gc.C_CYAN)

	testUnicode()

	PrintStatus("Terminal initialized successfully")
}

// EndTerminal cleans up the terminal and exits the program.
func EndTerminal() {
	gc.End()
	os.Exit(0)
}

func GetMaxRows() int {
	// Must be called only after ncurses has been initialized.
	log.Assert(Stdscr != nil)

	maxY, _ := Stdscr.MaxYX()
	return maxY
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
	log.Assert(Stdscr != nil)
	log.Assert(len(format) > 0)

	if len(params) == 0 {
		Stdscr.MovePrintf(GetMaxRows()-1, 0, ">> "+format)
	} else {
		Stdscr.MovePrintf(GetMaxRows()-1, 0, ">> "+format, params...)
	}

	Stdscr.ClearToEOL()
	Stdscr.Refresh()
}

func ClearScreen() {
	// Must be called only after ncurses has been initialized.
	log.Assert(Stdscr != nil)

	Stdscr.Clear()
	PrintStatus("Screen cleared")
	Stdscr.Refresh()
}
