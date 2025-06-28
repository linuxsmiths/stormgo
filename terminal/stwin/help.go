package stwin

import (
	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

func DrawHelp(sw *STWin) {
	// Must be called only for the help window.
	log.Assert(sw.IsHelpWindow())
	// Caller must have allocated the goncurses window.
	log.Assert(sw.Window != nil)

	win := sw.Window

	// Draw a box around the help window.
	err := win.Box(0, 0)
	log.Assert(err == nil, err)

	// Print the copyright and version information.
	y := 1
	x := 1
	cs := common.GetCopyright()

	win.ColorOn(stlib.YellowOnBlack)
	for _, line := range cs {
		win.MovePrint(y, x, line)
		y++
	}
	win.ColorOff(stlib.YellowOnBlack)

	// Print help on mouse usage.
	y++
	win.MovePrint(y, x, "Mouse:")

	mouseHelpKey := []string{
		"left click on a window",
		"left click on a column header",
		"left click and drag",
	}

	mouseHelpAction := []string{
		"select the window",
		"sort the table by that column",
		"move the window (release to place)",
	}

	xsplit := 35

	for i, key := range mouseHelpKey {
		y++
		x = 1
		win.ColorOn(stlib.CyanOnBlack)
		win.MovePrint(y, x, common.TruncateAndPadUTF8String(key+": ", xsplit))
		win.ColorOff(stlib.CyanOnBlack)
		x += xsplit
		win.MovePrint(y, x, mouseHelpAction[i])
	}

	// Print help on key usage.
	y += 2
	x = 1
	win.MovePrint(y, x, "Keyboard:")

	kbHelpKey := []string{
		"Tab",
		"h",
		"q",
	}

	kbHelpAction := []string{
		"focus on next window",
		"this help",
		"quit",
	}

	xsplit = 10
	for i, key := range kbHelpKey {
		y++
		x = 1
		win.ColorOn(stlib.CyanOnBlack)
		win.MovePrint(y, x, common.TruncateAndPadUTF8String(key+": ", xsplit))
		win.ColorOff(stlib.CyanOnBlack)
		x += xsplit
		win.MovePrint(y, x, kbHelpAction[i])
	}
}
