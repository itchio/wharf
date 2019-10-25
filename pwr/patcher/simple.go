package patcher

import (
	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/savior"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/pkg/errors"
)

type PatchFreshParams struct {
	PatchReader savior.SeekSource

	TargetDir string
	OutputDir string

	Consumer *state.Consumer
}

func PatchFresh(params PatchFreshParams) error {
	if params.PatchReader == nil {
		return errors.Errorf("PatchFreshParams.PatchReader can't be nil")
	}
	if params.TargetDir == "" {
		return errors.Errorf("PatchFreshParams.TargetDir can't be empty")
	}
	if params.OutputDir == "" {
		return errors.Errorf("PatchFreshParams.SourceDir can't be empty")
	}

	pat, err := New(params.PatchReader, params.Consumer)
	if err != nil {
		return err
	}

	targetPool := fspool.New(pat.GetTargetContainer(), params.TargetDir)

	bwl, err := bowl.NewFreshBowl(bowl.FreshBowlParams{
		TargetContainer: pat.GetTargetContainer(),
		TargetPool:      targetPool,

		SourceContainer: pat.GetSourceContainer(),
		OutputFolder:    params.OutputDir,
	})
	if err != nil {
		return err
	}

	err = pat.Resume(nil, targetPool, bwl)
	if err != nil {
		return err
	}

	err = bwl.Commit()
	if err != nil {
		return err
	}

	return nil
}
