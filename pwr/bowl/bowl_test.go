package bowl_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/arkive/zip"

	"github.com/itchio/lake/pools/zipwriterpool"
	"github.com/itchio/wharf/pwr/bowl"
)

func Test_PatchOneSameLength(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("leaf"))
		},
	})
}

func Test_PatchOneSameContents(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("moon"))
		},
	})
}

func Test_PatchOneLonger(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("moon and stars"))
		},
	})
}

func Test_PatchOneShorter(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("nothing makes sense"))
		},
	})
}

func Test_PatchOneAddTwo(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("moon and stars"))
			p.patch("sticks", []byte("not my tempo"))
			p.patch("bones", []byte("it's just a yard, don't be so grave"))
		},
	})
}

func Test_RenameOneA(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("fleeting", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("fleeting", "that/is/pretty/deep")
		},
	})
}

func Test_RenameOneB(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("that/is/pretty/deep", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("that/is/pretty/deep", "for/an/egg")
		},
	})
}

func Test_RemoveHalf(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("one", []byte("i'm alive"))
			p.file("two", []byte("im dead"))
			p.file("three", []byte("i live on"))
			p.file("four", []byte("my legacy will live on"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("one", "one")
			p.transpose("three", "three")
		},
	})
}

func Test_DuplicateOne(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("fool", []byte("oh?"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("fool", "interview/fool")
			p.transpose("fool", "senseless/fool")
		},
	})
}

func Test_DuplicateTwo(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("foo", []byte("these names are often chosen by"))
			p.file("bar", []byte("...developers whose imagination has run dry"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("foo", "somewhere/over/the/foo1")
			p.transpose("foo", "rainbow/foo2")
			p.transpose("bar", "somewhere/over/the/bar1")
			p.transpose("bar", "hedge/bar2")
		},
	})
}

func Test_Swaperoo(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("foodir/foo", []byte("these names are often chosen by"))
			p.file("bardir/bar", []byte("...developers whose imagination has run dry"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("foodir/foo", "bardir/bar")
			p.transpose("bardir/bar", "foodir/foo")
		},
	})
}

func Test_AllTogetherNow(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("peaceful", []byte("i am not to be disturbed"))
			p.file("patched", []byte("moon"))
			p.file("mover/visitor", []byte("i'm going somewhere"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("peaceful", "peaceful")
			p.patch("patched", []byte("moonish"))
			p.transpose("mover/visitor", "shaker/visitor")
		},
	})
}

func runScenario(t *testing.T, params *bowlerParams) {
	// dry bowl
	params.makeBowl = func(p *makeBowlParams) (bowl.Bowl, bowlMode) {
		b, err := bowl.NewDryBowl(&bowl.DryBowlParams{
			SourceContainer: p.SourceContainer,
			TargetContainer: p.TargetContainer,
		})
		must(t, err)

		return b, bowlModeNoop
	}
	t.Run("dryBowl", func(t *testing.T) {
		runBowler(t, params)
	})

	// fresh bowl
	params.makeBowl = func(p *makeBowlParams) (bowl.Bowl, bowlMode) {
		b, err := bowl.NewFreshBowl(&bowl.FreshBowlParams{
			SourceContainer: p.SourceContainer,
			TargetContainer: p.TargetContainer,
			TargetPool:      p.TargetPool,
			OutputFolder:    p.FreshFolder,
		})
		must(t, err)

		return b, bowlModeFresh
	}
	t.Run("freshBowl", func(t *testing.T) {
		runBowler(t, params)
	})

	// overlay bowl
	params.makeBowl = func(p *makeBowlParams) (bowl.Bowl, bowlMode) {
		b, err := bowl.NewOverlayBowl(&bowl.OverlayBowlParams{
			SourceContainer: p.SourceContainer,
			TargetContainer: p.TargetContainer,
			OutputFolder:    p.TargetFolder,
			StageFolder:     p.FreshFolder,
		})
		must(t, err)

		return b, bowlModeInPlace
	}
	t.Run("overlayBowl", func(t *testing.T) {
		runBowler(t, params)
	})

	// pool bowl
	params.makeBowl = func(p *makeBowlParams) (bowl.Bowl, bowlMode) {
		p.ZipFilePath = filepath.Join(p.FreshFolder, "archive.zip")
		zipFile, err := os.Create(p.ZipFilePath)
		must(t, err)

		zw := zip.NewWriter(zipFile)

		p.Cleanup = func() {
			must(t, zipFile.Close())
		}

		wp := zipwriterpool.New(p.SourceContainer, zw)

		b, err := bowl.NewPoolBowl(&bowl.PoolBowlParams{
			TargetContainer: p.TargetContainer,
			TargetPool:      p.TargetPool,
			SourceContainer: p.SourceContainer,
			OutputPool:      wp,
		})
		must(t, err)

		return b, bowlModeZip
	}
	t.Run("poolBowl", func(t *testing.T) {
		runBowler(t, params)
	})
}
