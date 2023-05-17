package awswrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyntypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type AWSWrapper struct {
	kinesisStreamName *string
	dynamodbTableName string
	bucketName        *string
	queueUrl          *string

	kinesisClient  *kinesis.Client
	dynamodbClient *dynamodb.Client
	s3client       *s3.Client
	sqs            *sqs.Client
	sns            *sns.Client
}

func New(ctx context.Context, streamName, tableName, bucketName, queueUrl string) (*AWSWrapper, error) {
	conf, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := &AWSWrapper{
		kinesisStreamName: aws.String(streamName),
		dynamodbTableName: tableName,
		bucketName:        &bucketName,
		queueUrl:          aws.String(queueUrl),

		kinesisClient:  kinesis.NewFromConfig(conf),
		dynamodbClient: dynamodb.NewFromConfig(conf),
		s3client:       s3.NewFromConfig(conf),
		sqs:            sqs.NewFromConfig(conf),
		sns:            sns.NewFromConfig(conf),
	}

	return client, nil
}

type KinesisRecord struct {
	Name string `json:"name,omitempty"`
}

func (a *AWSWrapper) PutKinesisRecordWrapper(ctx context.Context, record *KinesisRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	input := &kinesis.PutRecordInput{
		Data:         data,
		PartitionKey: aws.String("1"),
		StreamName:   a.kinesisStreamName,
	}

	_, err = a.kinesisClient.PutRecord(ctx, input)

	return err
}

func (a *AWSWrapper) ListShardsWrapper(ctx context.Context, streamName string) error {
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	_, err := a.kinesisClient.ListShards(ctx, input)

	return err
}

func (a *AWSWrapper) GetItemWrapper(ctx context.Context, key string) error {
	input := &dynamodb.GetItemInput{
		Key: map[string]dyntypes.AttributeValue{
			"PK": &dyntypes.AttributeValueMemberS{Value: key},
		},
		TableName: aws.String(a.dynamodbTableName),
	}

	_, err := a.dynamodbClient.GetItem(ctx, input)

	return err
}

func (a *AWSWrapper) QueryTableWrapper(ctx context.Context, key string, prefix string) error {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(a.dynamodbTableName),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]string{
			"pk": "SK",
			"sk": "GSI1SK",
		},
		ExpressionAttributeValues: map[string]dyntypes.AttributeValue{
			":pk": &dyntypes.AttributeValueMemberS{Value: key},
			":sk": &dyntypes.AttributeValueMemberS{Value: prefix},
		},

		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :sk)"),
		ReturnConsumedCapacity: dyntypes.ReturnConsumedCapacityTotal,
	}

	_, err := a.dynamodbClient.Query(ctx, input)
	return err
}

func (a *AWSWrapper) PutObjectWrapper(ctx context.Context, key string, obj *S3Object) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	input := createS3PutObjectInput(a.bucketName, key, bytes.NewReader(data))

	_, err = a.s3client.PutObject(ctx, input)

	return err
}

func (a *AWSWrapper) ReceiveMessageWrapper(ctx context.Context) error {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:              a.queueUrl,
		AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"ID", "COUNTRY"},
		VisibilityTimeout:     20,
		WaitTimeSeconds:       30,
	}

	_, err := a.sqs.ReceiveMessage(ctx, input)
	return err
}

func (a *AWSWrapper) PublishMessageWrapper(ctx context.Context) error {
	input := &sns.PublishInput{
		Message: aws.String("message"),
	}

	_, err := a.sns.Publish(ctx, input)

	return err
}

func createS3PutObjectInput(bucket *string, key string, body io.Reader) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket: bucket,
		Body:   body,
		Key:    aws.String(key),
	}
}

type S3Object struct {
	Name string `json:"name"`
}
