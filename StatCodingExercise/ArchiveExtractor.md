The ArchiveExtractor class has a dependency to satisfy for successfull execution. This class was built to interface with an S3 Bucket for access to the files to process.

You **MUST** have an *auth_data.json* file located at {root}/StatCodingExercise/StatCodingExercise/auth_data.json (at the 'project' level, as a sibling of this markdown file).

The auth_data.json file is expected to have the following structure:

    {
      "STORAGE": {
        "REGION": "",       // string name of the S3 Region the bucket is in
        "BUCKET": "",       // string name of the actual storage bucket to connect to
        "ACCESS_KEY": "",   // string value of the Access Key authorized to connect to the bucket
        "SECRET": ""        // string value of the Secret to use with the Access Key when connecting
      }
    }
