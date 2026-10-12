package bowl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
)

// layout returns an install folder, a stage folder and a sibling directory
// that nothing must ever write into.
func layout(t *testing.T) (output, stage, outside string) {
	t.Helper()
	root := t.TempDir()
	output = filepath.Join(root, "install")
	stage = filepath.Join(root, "stage")
	outside = filepath.Join(root, "outside")
	for _, d := range []string{output, stage, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return
}

func assertOutsideUntouched(t *testing.T, outside string) {
	t.Helper()
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("outside directory was written to: %v", entries)
	}
}

func writeThrough(t *testing.T, b Bowl, index int64, data string) {
	t.Helper()
	w, err := b.GetWriter(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Resume(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func newOverlay(t *testing.T, target, source *tlc.Container, output, stage string) Bowl {
	t.Helper()
	b, err := NewOverlayBowl(OverlayBowlParams{
		TargetContainer: target,
		SourceContainer: source,
		OutputFolder:    output,
		StageFolder:     stage,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A target that was installed earlier has turned "d" into a symlink pointing
// outside the install folder. "d" is deliberately absent from Dirs, so nothing
// replaces the link before the move; only the containment check stands in
// the way.
func Test_OverlayBowlRefusesSymlinkedParent(t *testing.T) {
	output, stage, outside := layout(t)
	if err := os.Symlink(outside, filepath.Join(output, "d")); err != nil {
		t.Skip(err)
	}
	source := &tlc.Container{Files: []*tlc.File{{Path: "d/escape.txt", Mode: 0o644, Size: 5}}}
	b := newOverlay(t, &tlc.Container{}, source, output, stage)
	writeThrough(t, b, 0, "hello")
	if err := b.Commit(); !errors.Is(err, tlc.ErrPathEscape) {
		t.Fatalf("commit through a symlinked parent: %v", err)
	}
	assertOutsideUntouched(t, outside)
}

// The same patch creates the link and the file under it.
func Test_OverlayBowlRefusesWriteUnderNewSymlink(t *testing.T) {
	output, stage, outside := layout(t)
	os.MkdirAll(filepath.Join(output, "real"), 0o755)
	source := &tlc.Container{
		Dirs:     []*tlc.Dir{{Path: "real", Mode: 0o755 | uint32(os.ModeDir)}},
		Symlinks: []*tlc.Symlink{{Path: "d", Dest: "real", Mode: 0o777 | uint32(os.ModeSymlink)}},
		Files:    []*tlc.File{{Path: "d/x", Mode: 0o644, Size: 5}},
	}
	b := newOverlay(t, &tlc.Container{}, source, output, stage)
	writeThrough(t, b, 0, "hello")
	if err := b.Commit(); !errors.Is(err, tlc.ErrPathEscape) {
		t.Fatalf("write under a link created in the same commit: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(output, "real", "x")); !os.IsNotExist(err) {
		t.Fatal("file was written through the new symlink")
	}
	assertOutsideUntouched(t, outside)
}

func Test_OverlayBowlRefusesEscapingSymlinkTargets(t *testing.T) {
	for _, dest := range []string{"/etc/hostname", "../../outside/x", "/Windows/x"} {
		output, stage, outside := layout(t)
		source := &tlc.Container{Symlinks: []*tlc.Symlink{{Path: "link", Dest: dest, Mode: 0o777 | uint32(os.ModeSymlink)}}}
		b := newOverlay(t, &tlc.Container{}, source, output, stage)
		if err := b.Commit(); !errors.Is(err, tlc.ErrPathEscape) {
			t.Fatalf("target %q accepted: %v", dest, err)
		}
		if _, err := os.Lstat(filepath.Join(output, "link")); !os.IsNotExist(err) {
			t.Fatalf("target %q: link was created", dest)
		}
		assertOutsideUntouched(t, outside)
	}
}

// A rename whose old or new side escapes must not move anything.
func Test_OverlayBowlRefusesEscapingTransposition(t *testing.T) {
	output, stage, outside := layout(t)
	os.WriteFile(filepath.Join(output, "a.txt"), []byte("data"), 0o644)
	os.WriteFile(filepath.Join(outside, "victim.txt"), []byte("keep"), 0o644)
	target := &tlc.Container{Files: []*tlc.File{{Path: "a.txt", Mode: 0o644, Size: 4}}}
	source := &tlc.Container{Files: []*tlc.File{{Path: "../outside/victim.txt", Mode: 0o644, Size: 4}}}
	b := newOverlay(t, target, source, output, stage)
	if err := b.Transpose(Transposition{TargetIndex: 0, SourceIndex: 0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); !errors.Is(err, tlc.ErrPathEscape) {
		t.Fatalf("escaping transposition: %v", err)
	}
	if data, _ := os.ReadFile(filepath.Join(outside, "victim.txt")); string(data) != "keep" {
		t.Fatal("file outside was overwritten")
	}
}

// A ghost under a parent that is now a symlink is skipped, not deleted
// through the link.
func Test_OverlayBowlGhostUnderSymlinkIsSkipped(t *testing.T) {
	output, stage, outside := layout(t)
	os.WriteFile(filepath.Join(outside, "keep.txt"), []byte("keep"), 0o644)
	if err := os.Symlink(outside, filepath.Join(output, "d")); err != nil {
		t.Skip(err)
	}
	target := &tlc.Container{Files: []*tlc.File{{Path: "d/keep.txt", Mode: 0o644, Size: 4}}}
	b := newOverlay(t, target, &tlc.Container{}, output, stage)
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(outside, "keep.txt")); err != nil {
		t.Fatal("ghost removal followed the symlink")
	}
}

// An overlay (in-place patch) of a path that is a foreign symlink on disk.
func Test_OverlayBowlRefusesOverlayThroughLeafSymlink(t *testing.T) {
	output, stage, outside := layout(t)
	victim := filepath.Join(outside, "victim.txt")
	os.WriteFile(victim, []byte("keep"), 0o644)
	if err := os.Symlink(victim, filepath.Join(output, "save")); err != nil {
		t.Skip(err)
	}
	target := &tlc.Container{Files: []*tlc.File{{Path: "save", Mode: 0o644, Size: 4}}}
	source := &tlc.Container{Files: []*tlc.File{{Path: "save", Mode: 0o644, Size: 4}}}
	b := newOverlay(t, target, source, output, stage)
	// Reading the target through the leaf symlink is refused at GetWriter.
	if _, err := b.GetWriter(0); !errors.Is(err, tlc.ErrPathEscape) {
		t.Fatalf("overlay of a leaf symlink: %v", err)
	}
	if data, _ := os.ReadFile(victim); string(data) != "keep" {
		t.Fatal("file outside was modified")
	}
}

// A symlink already sitting at a staging path must be replaced, not written
// through.
func Test_OverlayBowlStageWriterReplacesLeafSymlink(t *testing.T) {
	output, stage, outside := layout(t)
	victim := filepath.Join(outside, "victim.txt")
	os.WriteFile(victim, []byte("keep"), 0o644)
	if err := os.Symlink(victim, filepath.Join(stage, "new.txt")); err != nil {
		t.Skip(err)
	}
	source := &tlc.Container{Files: []*tlc.File{{Path: "new.txt", Mode: 0o644, Size: 5}}}
	b := newOverlay(t, &tlc.Container{}, source, output, stage)
	writeThrough(t, b, 0, "hello")
	if data, _ := os.ReadFile(victim); string(data) != "keep" {
		t.Fatal("stage writer wrote through the symlink")
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(filepath.Join(output, "new.txt")); string(data) != "hello" {
		t.Fatal("file was not installed")
	}
}

func Test_FreshBowlRefusesEscapingPath(t *testing.T) {
	output, _, outside := layout(t)
	source := &tlc.Container{Files: []*tlc.File{{Path: "../outside/escape.txt", Mode: 0o644, Size: 5}}}
	// Prepare runs inside NewFreshBowl; that is where the escape used to happen.
	_, err := NewFreshBowl(FreshBowlParams{
		TargetContainer: &tlc.Container{},
		SourceContainer: source,
		TargetPool:      fspool.New(&tlc.Container{}, output),
		OutputFolder:    output,
	})
	if !errors.Is(err, tlc.ErrPathEscape) {
		t.Fatalf("escaping path accepted: %v", err)
	}
	assertOutsideUntouched(t, outside)
}
