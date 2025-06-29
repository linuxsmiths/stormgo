package stwin

import (
	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
)

// Caller must ensure that sw.Window is refreshed after we return, o/w the
// content will not show on the screen.
func DrawPad(sw *STWin) {
	// Must be called only for a pad.
	log.Assert(sw.IsPad())
	// Must be a valid pad with valid content.
	log.Assert(sw.PW > 0 && sw.PH > 0, sw.PW, sw.PH)
	log.Assert(sw.PY >= 0 && sw.PX >= 0, sw.PY, sw.PX)
	log.Assert(sw.PY < sw.PH && sw.PX < sw.PW, sw.PY, sw.PX, sw.PH, sw.PW)

	// Caller must have allocated the goncurses window.
	win := sw.Window
	log.Assert(win != nil)

	// Draw a box around the help window.
	err := win.Box(0, 0)
	log.Assert(err == nil, err)

	pad := sw.Pad

	stlib.PrintStatus("PH: %d, PW: %d, PY: %d, PX: %d, Y: %d, X: %d, H: %d, W: %d",
		sw.PH, sw.PW, sw.PY, sw.PX, sw.Y, sw.X, sw.H, sw.W)

	//
	// Visible area is the minimum of the scrollable area and the pad.
	// The visible height is sw.H-2 and the visible width is sw.W-2.
	//
	visibleRows := sw.H - 2
	visibleRows = min(visibleRows, sw.PH-sw.PY)
	visibleCols := sw.W - 2
	visibleCols = min(visibleCols, sw.PW-sw.PX)

	log.Assert(visibleRows > 0 && visibleCols > 0, visibleRows, visibleCols)

	// Copy visible portion of the pad to the goncurses window.
	err = win.Copy(pad.Window, sw.PY, sw.PX, 1, 1, visibleRows, visibleCols, true)
	if err != nil {
		log.Assert(false, err, sw.PY, sw.PX, sw.PH, sw.PW, sw.H, sw.W)
	}
}

func PadHandleKey(sw *STWin, key gc.Key) {
	log.Assert(sw.IsPad())

	switch key {
	case 'q':
		log.Assert(false)
	case gc.KEY_UP:
		if sw.PY > 0 {
			sw.PY--
		}
	case gc.KEY_DOWN:
		// sw.H-2 is the height of the visible/scrollable area.
		lastRowInPad := sw.PH - 1
		lastRowInVisibleArea := sw.PY + (sw.H - 2) - 1
		// If last possible row is already visible, do not scroll down.
		if lastRowInPad > lastRowInVisibleArea {
			sw.PY++
			log.Assert(sw.PY < sw.PH, sw.PY, sw.PH)
		}
	case gc.KEY_LEFT:
		if sw.PX > 0 {
			sw.PX--
		}
	case gc.KEY_RIGHT:
		// sw.W-2 is the width of the visible/scrollable area.
		lastColInPad := sw.PW - 1
		lastColInVisibleArea := sw.PX + (sw.W - 2) - 1
		// If last possible col is already visible, do not scroll right.
		if lastColInPad > lastColInVisibleArea {
			sw.PX++
			log.Assert(sw.PX < sw.PW, sw.PX, sw.PW)
		}
	}
}
