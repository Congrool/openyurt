/*
Copyright 2020 The OpenYurt Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package yurtinit

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/openyurtio/openyurt/pkg/yurtctl/cmd/yurtinit/kind"
	"github.com/openyurtio/openyurt/pkg/yurtctl/cmd/yurtinit/sealer"
)

func NewCmdInit() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize the OpenYurt cluster.",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()
			if err != nil {
				return err
			}
			return errors.New("subcommand is required")
		},
		Args: cobra.NoArgs,
	}

	cmd.AddCommand(kind.NewKindInitCMD())
	cmd.AddCommand(sealer.NewSealerInitCMD())

	return cmd
}
