pipeline {
    agent {
        label 'backend_terminating_j8 || builder-backend-j8 || general_terminating_j8'
    }

    parameters {
        string(name: "BRANCH_REV", defaultValue: "v8.0.0-prw-sink", description: "Revision to build")
        string(name: "IMAGE_TAG", defaultValue: "v8.0.0-4", description: "Image tag")
        booleanParam(name: 'DRY_RUN', defaultValue: true, description: 'Perform a dry run (does not push images)')
    }

    environment {
        ARTIFACTORY_REPO="docker.internal.sysdig.com"
        IMAGE_NAME_PREFIX = "${env.ARTIFACTORY_REPO}/docker/veneur-rw"
        IMAGE_NAME = "${env.IMAGE_NAME_PREFIX}:${params.IMAGE_TAG}"
        IMAGE_NAME_ALPINE = "${env.IMAGE_NAME_PREFIX}:${params.IMAGE_TAG}-alpine3.10"
    }

    stages {

        stage('Build') {
            steps {
                script {
                    dir('public-docker-images') {
                        sh "docker build -f Dockerfile-debian-sid --build-arg BUILD_REF=${params.BRANCH_REV} -t ${env.IMAGE_NAME} ."
                        sh "docker build -f Dockerfile-alpine --build-arg BUILD_REF=${params.BRANCH_REV} -t ${env.IMAGE_NAME_ALPINE} ."
                    }
                }
            }
        }

        stage('Publish image') {
            steps {
                script {
                    withCredentials([usernamePassword(credentialsId: 'jenkins-artifactory', usernameVariable: 'ARTIFACTORY_USERNAME', passwordVariable:'ARTIFACTORY_PASSWORD')]) {
                        sh "docker login -u=$ARTIFACTORY_USERNAME -p=$ARTIFACTORY_PASSWORD ${env.ARTIFACTORY_REPO}"
                        if (params.DRY_RUN) {
                            echo "docker push ${env.IMAGE_NAME}"
                            echo "docker push ${env.IMAGE_NAME_ALPINE}"
                        } else {
                            sh "docker push ${env.IMAGE_NAME}"
                            sh "docker push ${env.IMAGE_NAME_ALPINE}"
                        }
                    }
                }
            }
        }
    }

    post {
        success {
            echo 'All done.'
        }
        cleanup {
            echo 'Cleaning up...'
            script {
                sh "docker rm ${env.IMAGE_NAME}"
                sh "docker rm ${env.IMAGE_NAME_ALPINE}"
            }
            cleanWs()
        }
    }
}