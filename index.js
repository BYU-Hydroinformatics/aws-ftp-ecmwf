"use strict"

const winston = require("winston")
const WinstonCloudWatch = require("winston-cloudwatch")
const AWS = require("aws-sdk") // eslint-disable-line import/no-extraneous-dependencies
var PromiseFtp = require("promise-ftp")

const ftp = new PromiseFtp()
const s3Stream = require("s3-upload-stream")(new AWS.S3())
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

winston.add(
  new WinstonCloudWatch({
    awsRegion: "us-west-2",
    logGroupName: "ftp_transfer_to_s3_ec2",
    logStreamName: logStreamName,
    jsonMessage: true
  })
)

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

  var upload = s3Stream.upload({
    Bucket: s3Bucket,
    Key: file.name
  })
  // upload.maxPartSize(153000000) // 150 MB
  // upload.concurrentParts(15)
  winston.info("Streaming " + file.name + " To S3 bucket " + s3Bucket)

  console.log("Streaming " + file.name + " To S3 bucket " + s3Bucket)

  return new Promise(function(resolve, reject) {
    stream.pipe(upload)

    upload.on("part", function(details) {
      winston.info(details)
      console.log(details)
    })

    upload.on("error", (err) => {
      winston.error("Error whith S3 stream")
      console.error("Error whith S3 stream")
      console.error(err)
      var myErrorObj = {
        errorType: "InternalServerError",
        httpStatus: 500,
        message: "Error whith S3 upload: " + err
      }
      stream.end()
      upload.end()
      reject(myErrorObj)
      return
    })

    upload.on("uploaded", function(details) {
      console.log("I am called")
      winston.info(file.name + " sent to S3 bucket " + s3Bucket)
      console.log(file.name + " sent to S3 bucket " + s3Bucket)
      stream.end()
      upload.end()
      resolve(details)
      return
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
      process.exit(0)
    }

    let finalResult = 0
    for (let file of filesNotOnS3) {
      let result = await moveFileToS3(file)
      console.log(result)
    }

    await ftp.end()
    winston.info("FTP Connection terminated. All transfers done. Exiting")
    process.exit(0)
  } catch (err) {
    winston.err(err)
    console.log(err)
    winston.info("Process ran into an error.")
    process.exit(1)
  }
}
main()
