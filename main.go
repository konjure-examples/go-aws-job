package main

import (
	"context"
	"os"

	"github.com/konjure-examples/go-aws-job/internal/awswrapper"
)

const streamNameEnvKey = "KINESIS_STREAM_NAME"

const dynamodbTableName = "dynamodb_table"

func main() {
	streamName := os.Getenv(streamNameEnvKey)
	bucketName := os.Getenv("S3" + "_BUCKET")
	queueUrl := os.Getenv("SQS_URL")
	ctx := context.Background()
	wrapper, err := awswrapper.New(ctx, streamName, dynamodbTableName, bucketName, queueUrl)
	if err != nil {
		return
	}

	err = wrapper.PutKinesisRecordWrapper(ctx, &awswrapper.KinesisRecord{Name: "Record Name"})
	if err != nil {
		return
	}

	err = wrapper.ListShardsWrapper(ctx, streamName)
	if err != nil {
		return
	}

	err = wrapper.GetItemWrapper(ctx, "item_id")
	if err != nil {
		return
	}

	err = wrapper.QueryTableWrapper(ctx, "ID", "PREFIX")
	if err != nil {
		return
	}

	err = wrapper.PutObjectWrapper(ctx, "s3_file.json", &awswrapper.S3Object{Name: "Object Name"})
	if err != nil {
		return
	}

	err = wrapper.ReceiveMessageWrapper(ctx)
	if err != nil {
		return
	}

	err = wrapper.PublishMessageWrapper(ctx)
	if err != nil {
		return
	}

}
