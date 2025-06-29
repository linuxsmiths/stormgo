package stwin

import (
	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

// Add help content to the pad.
// Note that the pad can have more content than what the terminal can display
// at once, so the user can scroll through it. See DrawPad().
func DrawHelp(sw *STWin) {
	// Must be called only for the help window.
	log.Assert(sw.IsHelpWindow())

	// Caller must have allocated the goncurses pad.
	pad := sw.Pad
	log.Assert(pad != nil)

	// Pad width and height, will be set according to the content.
	padW := 0
	padH := 0

	// Print the copyright and version information.
	y := 0
	x := 0
	cs := common.GetCopyright()

	pad.ColorOn(stlib.YellowOnBlack)
	for _, line := range cs {
		pad.MovePrint(y, x, line)
		y++
		padW = max(padW, x+len(line))
	}
	pad.ColorOff(stlib.YellowOnBlack)

	// Print help on mouse usage.
	y++
	pad.MovePrint(y, x, "Mouse:")
	y++

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
		x = 1
		pad.ColorOn(stlib.CyanOnBlack)
		pad.MovePrint(y, x, common.TruncateAndPadUTF8String(key+": ", xsplit))
		pad.ColorOff(stlib.CyanOnBlack)
		x += xsplit
		pad.MovePrint(y, x, mouseHelpAction[i])
		y++

		padW = max(padW, x+len(mouseHelpAction[i]))
	}

	// Print help on key usage.
	y++
	x = 1
	pad.MovePrint(y, x, "Keyboard:")
	y++

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
		x = 1
		pad.ColorOn(stlib.CyanOnBlack)
		pad.MovePrint(y, x, common.TruncateAndPadUTF8String(key+": ", xsplit))
		pad.ColorOff(stlib.CyanOnBlack)
		x += xsplit
		pad.MovePrint(y, x, kbHelpAction[i])
		y++

		padW = max(padW, x+len(kbHelpAction[i]))
	}

	padH = y

	sw.PH = padH
	sw.PW = padW

	sw.Pad.Resize(padH, padW)
}
