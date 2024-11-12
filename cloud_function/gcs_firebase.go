package p

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"google.golang.org/api/option"
)

type ValidationResult struct {
	IsValid             bool
	ErrorMessage        string
	ValidationTimestamp time.Time
}

func validateRecord(record map[string]interface{}) ValidationResult {
	if _, ok := record["name"]; !ok || record["name"] == "" {
		return ValidationResult{
			IsValid:             false,
			ErrorMessage:        "Name is required.",
			ValidationTimestamp: time.Now(),
		}
	}

	if _, ok := record["age"]; !ok || !isInteger(record["age"]) || record["age"].(int64) < 0 {
		return ValidationResult{
			IsValid:             false,
			ErrorMessage:        "Age must be a non-negative integer.",
			ValidationTimestamp: time.Now(),
		}
	}

	if _, ok := record["salary"]; !ok || !isNumber(record["salary"]) || record["salary"].(float64) < 0 {
		return ValidationResult{
			IsValid:             false,
			ErrorMessage:        "Salary must be a non-negative number.",
			ValidationTimestamp: time.Now(),
		}
	}

	return ValidationResult{
		IsValid:             true,
		ValidationTimestamp: time.Now(),
	}
}

func isInteger(value interface{}) bool {
	_, ok := value.(int64)
	return ok
}

func isNumber(value interface{}) bool {
	_, ok := value.(float64)
	return ok
}

func readParquetFile(bucketName, filePath string) ([]map[string]interface{}, error) {
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	if err != nil {
		return nil, err
	}

	rc, err := storageClient.Bucket(bucketName).Object(filePath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	reader, err := ipc.NewFileReader(rc)
	if err != nil {
		return nil, err
	}

	var records []map[string]interface{}
	for i := 0; i < reader.NumRecords(); i++ {
		record := make(map[string]interface{})
		row, err := reader.Read()
		if err != nil {
			return nil, err
		}

		for j, field := range reader.Schema().Fields() {
			record[field.Name()] = row.Value(j)
		}
		records = append(records, record)
	}

	return records, nil
}

func writeToFirestore(ctx context.Context, collectionName string, data []map[string]interface{}) error {
	fsClient, err := firestore.NewClient(ctx, os.Getenv("GOOGLE_PROJECT_ID"))
	if err != nil {
		return err
	}
	defer fsClient.Close()

	batch := fsClient.Batch()
	for _, record := range data {
		docRef := fsClient.Collection(collectionName).Doc(fmt.Sprintf("%d", hash(record)))
		batch.Set(docRef, record)
	}
	_, err = batch.Commit(ctx)
	return err
}

func hash(record map[string]interface{}) int64 {
	var hash int64
	for k, v := range record {
		hash += int64(len(k)) * int64(arrow.Int64Type.BitWidth())
		hash += int64(fmt.Sprintf("%v", v))
	}
	return hash
}

func ProcessParquetFile(ctx context.Context, m map[string]interface{}) (string, error) {
	bucketName := os.Getenv("GCS_BUCKET_NAME")
	firestoreCollection := os.Getenv("FIRESTORE_COLLECTION")
	filePath := m["filePath"].(string)

	records, err := readParquetFile(bucketName, filePath)
	if err != nil {
		return "", err
	}

	var validRecords []map[string]interface{}
	for _, record := range records {
		validationResult := validateRecord(record)
		if validationResult.IsValid {
			record["validationTimestamp"] = validationResult.ValidationTimestamp
			validRecords = append(validRecords, record)
		} else {
			return fmt.Sprintf("Invalid data in the Parquet file: %s", validationResult.ErrorMessage), fmt.Errorf("invalid data")
		}
	}

	if err := writeToFirestore(ctx, firestoreCollection, validRecords); err != nil {
		return "", err
	}

	return "Data processed and written to Firestore successfully.", nil
}
