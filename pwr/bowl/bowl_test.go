package bowl_test

import (
	"testing"

	"github.com/itchio/wharf/pwr/bowl"
)

func TestBowlPatchOneSameLength(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("leaf"))
		},
	})
}

func TestBowlPatchOneSameContents(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("moon"))
		},
	})
}

func TestBowlPatchOneLonger(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("moon"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("moon and stars"))
		},
	})
}

func TestBowlPatchOneShorter(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("patched", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.patch("patched", []byte("nothing makes sense"))
		},
	})
}

func TestBowlRenameOneA(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("fleeting", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("fleeting", "that/is/pretty/deep")
		},
	})
}

func TestBowlRenameOneB(t *testing.T) {
	runScenario(t, &bowlerParams{
		makeTarget: func(p *bowlerPreparator) {
			p.file("that/is/pretty/deep", []byte("nothing makes sense without the end"))
		},
		apply: func(p *bowlerSimulator) {
			p.transpose("that/is/pretty/deep", "for/an/egg")
		},
	})
}

func TestBowlDuplicateOne(t *testing.T) {
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

func TestBowlDuplicateTwo(t *testing.T) {
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

func TestBowlAllTogetherNow(t *testing.T) {
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
}
