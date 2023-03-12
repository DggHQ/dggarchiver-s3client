package dggarchivers3client

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

/*
S3Client Data Strucs
*/
type S3Client struct {
	Endpoint    string
	AccessKey   string
	SecretKey   string
	Bucket      string
	SSL         bool
	MinioClient *minio.Client
}

/*
Initialize a new S3Client
*/
func NewS3Client(endpoint string, accessKey string, secretKey string, bucket string, useSSL bool) (*S3Client, error) {
	minioClient, err := minio.New(
		endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: useSSL,
		})
	if err != nil {
		return &S3Client{}, err
	} else {
		return &S3Client{
			Endpoint:    endpoint,
			AccessKey:   accessKey,
			SecretKey:   secretKey,
			Bucket:      bucket,
			SSL:         useSSL,
			MinioClient: minioClient,
		}, nil
	}
}

/*
Uploads a local file to Minio/S3
*/
func (s3 *S3Client) UploadBlob(localFileURI string, objectName string) error {
	_, err := s3.MinioClient.FPutObject(context.Background(), s3.Bucket, objectName, localFileURI, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return err
	}
	return nil
}

/*
Downloads a blob from Minio/S3 to a local file
*/
func (s3 *S3Client) DownloadBlob(objectName string, outputURI string) error {
	err := s3.MinioClient.FGetObject(context.Background(), s3.Bucket, objectName, outputURI, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

/*
Deletes a remote blob from Minio/S3
*/
func (s3 *S3Client) DeleteBlob(objectName string) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}
	err := s3.MinioClient.RemoveObject(context.Background(), s3.Bucket, objectName, opts)
	if err != nil {
		return err
	}
	return nil
}
