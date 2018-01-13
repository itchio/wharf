package bowl_test

import (
	"testing"

	"github.com/itchio/wharf/pwr/bowl"
)

func TestBowl(t *testing.T) {
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
