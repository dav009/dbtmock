package main

import (
	"fmt"
	"log"
	"os"

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
				Required: true,
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
			fmt.Println("Using Manifest: ", manifestPath)
			m := dbtest.ParseManifest(manifestPath)
			fmt.Println("Parsed Manifest! ")
			fmt.Println("Parsing tests in: ", testsPath)
			t, err := dbtest.ParseTest(testsPath)
			if err != nil {
				return err
			}
			fmt.Println("Parsed Tests! ")
			fmt.Println("Generating Tests...")
			sqlCode, err := dbtest.GenerateTestSQL(t, m)
			if err != nil {
				return err
			}
			fmt.Println("")
			fmt.Println(fmt.Sprintf("%v", sqlCode))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
