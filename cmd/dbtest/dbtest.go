package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dav009/dbtest"
	cli "github.com/urfave/cli/v2"
)

func main() {

	app := &cli.App{
		Name:  "boom",
		Usage: "make an explosive entrance",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "manifest",
				Value:    "target/manifest.json",
				Usage:    "Path to your dbt's manifest.json",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "tests",
				Value:    "",
				Usage:    "Path to your folder containing json test definitions",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "output",
				Value:    "",
				Usage:    "Path where test queries will be gneerated",
				Required: true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			manifestPath := cCtx.String("manifest")
			testsPath := cCtx.String("tests")
			output := cCtx.String("output")
			fmt.Println("Using Manifest: ", manifestPath)
			m := dbtest.ParseManifest(manifestPath)
			fmt.Println("Parsed Manifest! ")
			fmt.Println("Parsing tests in: ", testsPath)
			tests, err := dbtest.ParseFolder(testsPath)
			if err != nil {
				return err
			}
			fmt.Println("Parsed Tests: ", len(tests))
			fmt.Println("Generating SQL...")
			for _, t := range tests {
				fmt.Println("Generating Test: ", t.Name)
				sqlCode, err := dbtest.GenerateTestSQL(t, m)
				if err != nil {
					return err
				}
				path := filepath.Join(output, t.Name+".sql")
				fmt.Println("Saving Test: ", path)
				dbtest.SaveSQL(path, sqlCode)
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
