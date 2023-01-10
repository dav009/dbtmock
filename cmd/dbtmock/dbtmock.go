package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dav009/dbtmock"
	cli "github.com/urfave/cli/v2"
)

func TestCommand() cli.Command {
	return cli.Command{
		Name:    "test",
		Aliases: []string{"t"},
		Usage:   "Run tests using a simulated BQ engine",
		Flags: []cli.Flag{&cli.StringFlag{
			Name:     "tests",
			Value:    "unit_tests/",
			Usage:    "Path to your folder containing json test definitions",
			Required: false,
		},
			&cli.StringFlag{
				Name:     "manifest",
				Value:    "target/manifest.json",
				Usage:    "Path to your dbt's manifest.json",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "mode",
				Value:    "local",
				Usage:    "whether to run test in local or cloud mode",
				Required: false,
			},
		},
		Action: func(cCtx *cli.Context) error {
			manifestPath := cCtx.String("manifest")
			mode := cCtx.String("mode")
			testsPath := cCtx.String("tests")
			fmt.Println("Using Manifest: ", manifestPath)
			m := dbtmock.ParseManifest(manifestPath)
			fmt.Println("Parsed Manifest! ")
			fmt.Println("Parsing tests in: ", testsPath)
			tests, err := dbtmock.ParseFolder(testsPath)
			if err != nil {
				return err
			}
			fmt.Println("Parsed Tests: ", len(tests))
			fmt.Println("Running Tests...")
			err = dbtmock.RunTests(mode, tests, m)
			if err != nil {
				fmt.Println("ERROR")
				return err
			}
			return nil
		},
	}
}

func GenerateSQL() cli.Command {
	return cli.Command{
		Name:    "generate",
		Aliases: []string{"g"},
		Usage:   "Run tests using a simulated BQ engine",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "manifest",
				Value:    "target/manifest.json",
				Usage:    "Path to your dbt's manifest.json",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "tests",
				Value:    "unit_tests/",
				Usage:    "Path to your folder containing json test definitions",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "output",
				Value:    "tests",
				Usage:    "Path where test queries will be gneerated",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "mode",
				Value:    "test",
				Usage:    "test mode: Generate SQL run during tests. simple: generates the sql code of a model",
				Required: false,
			},
		},
		Action: func(cCtx *cli.Context) error {
			manifestPath := cCtx.String("manifest")
			testsPath := cCtx.String("tests")
			output := cCtx.String("output")
			fmt.Println("Using Manifest: ", manifestPath)
			m := dbtmock.ParseManifest(manifestPath)
			fmt.Println("Parsed Manifest! ")
			fmt.Println("Parsing tests in: ", testsPath)
			tests, err := dbtmock.ParseFolder(testsPath)
			if err != nil {
				return err
			}
			fmt.Println("Parsed Tests: ", len(tests))
			fmt.Println("Generating SQL...")
			for _, t := range tests {
				fmt.Println("Generating Test: ", t.Name)
				sqlQueries, err := dbtmock.GenerateTestSQL(t, m)
				if err != nil {
					return err
				}

				path := filepath.Join(output, t.Name+"_"+"ExpectedMinusQuery"+".sql")
				fmt.Println("Saving Test: ", path)
				fmt.Println(sqlQueries.ExpectedMinusQuery)
				err = dbtmock.SaveSQL(path, sqlQueries.ExpectedMinusQuery)
				if err != nil {
					return err
				}

				path = filepath.Join(output, t.Name+"_"+"QueryMinusExpected"+".sql")
				fmt.Println("Saving Test: ", path)
				err = dbtmock.SaveSQL(path, sqlQueries.QueryMinusExpected)
				if err != nil {
					return err
				}

			}
			return nil
		},
	}
}

func main() {
	testCmd := TestCommand()
	genCmd := GenerateSQL()
	app := cli.NewApp()
	app.Name = "dbtmock"
	app.Usage = ""
	app.Commands = []*cli.Command{
		&testCmd,
		&genCmd,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
