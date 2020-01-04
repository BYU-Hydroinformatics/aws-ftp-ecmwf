"use strict"

const winston = require("winston")
const WinstonCloudWatch = require("winston-cloudwatch")
const AWS = require("aws-sdk") // eslint-disable-line import/no-extraneous-dependencies
var PromiseFtp = require("promise-ftp")

const ftp = new PromiseFtp()
const s3 = new AWS.S3()

const creds = {
  host: "ftp.ecmwf.int",
  user: "safer",
  password: "neo2008"
}

let dateObj = new Date()
let month = dateObj.getUTCMonth() + 1 //months from 1-12
let day = dateObj.getUTCDate()
let year = dateObj.getUTCFullYear()

let logStreamName = `${month}-${day}-${year}`

var logInstance = winston.add(
  new WinstonCloudWatch({
    name: "ecmwf_ftp",
    awsRegion: "us-west-2",
    logGroupName: "ftp_transfer_to_s3_ec2",
    logStreamName: logStreamName,
    jsonMessage: true,
    uploadRate: 500
  })
)

const waitForLogger = async (logger) => {
  return new Promise((resolve) => setTimeout(resolve, 2500))
}

async function quitProcess(code) {
  await ftp.end()
  winston.info("FTP Connection terminated. All transfers done. Exiting")
  await waitForLogger(winston)
  process.exit(code)
}

const s3Bucket = "ecmwf-files-byu-hydro-ecmwf"

function checkFileOnS3(fileName) {
  return s3
    .headObject({ Bucket: s3Bucket, Key: fileName })
    .promise()
    .then((res) => {
      // console.log(res)
      return Promise.resolve(false)
    })
    .catch((err) => {
      // console.log(err)
      // console.log("File Not found. ")
      return Promise.resolve(true)
    })
}

async function filter(arr, callback) {
  const fail = Symbol()
  return (await Promise.all(
    arr.map(async (item) => ((await callback(item)) ? item : fail))
  )).filter((i) => i !== fail)
}

async function moveFileToS3(file) {
  let size = await ftp.size(file.name)

  winston.info(`Downloading file ${file.name}, size: ${size}`)
  console.log(`Downloading file ${file.name}, size: ${size}`)
  let stream = await ftp.get(file.name)

  // Upload the stream
  var s3obj = new AWS.S3({ params: { Bucket: s3Bucket, Key: file.name } })

  winston.info("Streaming " + file.name + " To S3 bucket " + s3Bucket)
  console.log("Streaming " + file.name + " To S3 bucket " + s3Bucket)

  return new Promise(function(resolve, reject) {
    s3obj
      .upload({ Body: stream })
      .on("httpUploadProgress", function(evt) {
        winston.info(`Progress: ${evt.loaded}/${evt.total}`)
        console.log("Progress:", evt.loaded, "/", evt.total)
      })
      .send(function(err, data) {
        if (err) {
          winston.error("Error whith S3 stream")
          winston.error(err)
          console.log("An error occurred", err)
        }
        winston.info(file.name + " sent to S3 bucket " + s3Bucket)
        console.log(file.name + " sent to S3 bucket " + s3Bucket)
      })
  })
}

async function main() {
  try {
    await ftp.connect({ host: creds.host, user: creds.user, password: creds.password })

    await ftp.cwd("tcyc")

    const fileList = await ftp.list()
    let filteredList = fileList.filter((file) => {
      return file.name.match(/(Runoff.*)\w+/g)
    })
    const filesNotOnS3 = await filter(filteredList, async (file) => {
      let res = await checkFileOnS3(file.name)
      return res
    })

    if (filesNotOnS3.length < 1) {
      winston.info("No files to push. Quitting")
      console.log("No files to push. Quitting")
    } else {
      for (let file of filesNotOnS3) {
        let result = await moveFileToS3(file)
        console.log(result)
      }
    }
    quitProcess(0)
  } catch (err) {
    winston.err(err)
    console.log(err)
    winston.info("Process ran into an error.")
    quitProcess(0)
  }
}
main()
