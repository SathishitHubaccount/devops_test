pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                echo "start"
            }
        }
        stage('Clone repo') {
            steps {
                git branch: 'main',
                url: 'https://github.com/SathishitHubaccount/devops_test'
            }
        }
        stage("zip"){
            steps{
                sh "zip folder.zip lambda_function.py"
            }
        }
        stage("upload_to_s3"){
            steps{
                sh "aws s3api put-object --bucket sathis-devops-test1 --key folder.zip --body folder.zip"
            }
        }
        stage("uploading_lambda_code"){
            steps{
                sh "aws lambda update-function-code --function-name test --s3-bucket sathis-devops-test1 --s3-key folder.zip --region ap-south-1"
            }
        }
}

}