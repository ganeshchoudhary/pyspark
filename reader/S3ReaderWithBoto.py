import boto3

bucket_name = "kaleido-ds2"
aws_access_key = 'AKIA2QF73PNMLXGUEAE7'  # 'AKIA2QF73PNML34LVKFB' #'AKIA2QF73PNMO63W5R6A'
aws_secret_key = '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'  # 'dcPmtGG4wazhd81fYDAhXKwvqXjcI0Eg3KGVsPH0' #'c6IO75OzfW9wgJCbtEJ50rx0EVYPyszcWeaZdpkx'
aws_endpoint_region = "s3." + 'ap-south-1' + ".amazonaws.com"

bucket_name = bucket_name
s3_output_key = 'kaleido-credit/Sonata/incremental/batch_369/customer_info.csv'
filename = '../resources/test.csv'
# s3://kaleido-ds2/kaleido-credit/Sonata/Staging/batch_369/

def readWithClient(session):



    s3 = session.client('s3')
    response = s3.list_buckets()
    print(response['Buckets'])

    with open('../resources/hello.csv', 'wb') as data:
        s3.download_fileobj(bucket_name, s3_output_key, data)


def readWithResource(session):
    s3 = session.resource('s3')
    s3.meta.client.download_file(bucket_name, s3_output_key, '../resources/hello.txt')


if __name__ == '__main__':
    # Create your own session
    my_session = boto3.session.Session()

    # Now we can create low-level clients or resource clients from our custom session
    sqs = my_session.client('sqs')
    s3 = my_session.resource('s3')

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='ap-south-1'
    )

    readWithResource(session)
    readWithClient(session)
