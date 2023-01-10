package dbtmock

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/fatih/color"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func RunQueryMinusExpectation(ctx context.Context, client *bigquery.Client, query string) error {
	fmt.Println("qyerying...")
	q := client.Query((query))
	fmt.Println("reading....")
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	for {

		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		color.Green("-------------")
		for i, field := range it.Schema {
			record := fmt.Sprintf("%s : %v", field.Name, row[i])
			color.Green(record)

		}
		color.Green("-------------")
		//color.Green(strings.Join(columns, "\t"))
		//color.Green(fmt.Sprintf("%v", row))
		err = errors.New("Query returned extra data compared to expectation..")
	}

	return err
}

func RunExpectationMinusQuery(ctx context.Context, client *bigquery.Client, query string) error {
	it, err := client.Query((query)).Read(ctx)
	if err != nil {
		return err
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		color.Red("-------------")
		for i, field := range it.Schema {
			record := fmt.Sprintf("%s : %v", field.Name, row[i])
			color.Red(record)

		}
		color.Red("-------------")
		color.Red(fmt.Sprintf("%v", row))
		err = errors.New("Expected data was not fully completed..")

	}
	return err
}

func RunTests(mode string, tests []Test, m Manifest) error {
	ctx := context.Background()
	const (
		projectID = "fq-stage-bigquery"
		datasetID = "dataset1"
		routineID = "routine1"
	)
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		return err
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
				),
			),
		),
	); err != nil {
		return err
	}
	if err := bqServer.SetProject(projectID); err != nil {
		return err
	}
	testServer := bqServer.TestServer()
	defer testServer.Close()

	var client *bigquery.Client
	if mode == "local" {
		client, err = bigquery.NewClient(
			ctx,
			projectID,
			option.WithEndpoint(testServer.URL),
			option.WithoutAuthentication(),
		)
	} else {
		client, err = bigquery.NewClient(
			ctx,
			projectID,
		)
	}

	if err != nil {
		return err
	}
	defer client.Close()

	var lastErr error = nil

	for _, t := range tests {
		sqlQueries, err := GenerateTestSQL(t, m)

		if err != nil {
			return err
		}
		fmt.Println("Running: Query minus Expectation")
		fmt.Println(sqlQueries.QueryMinusExpected)
		fmt.Println("end of query..")
		err = RunQueryMinusExpectation(ctx, client, sqlQueries.QueryMinusExpected)
		if err != nil {
			lastErr = err
		}
		fmt.Println("Running:Expectation minus Query")
		err = RunExpectationMinusQuery(ctx, client, sqlQueries.ExpectedMinusQuery)
		if err != nil {
			lastErr = err
		}

	}
	return lastErr
}
