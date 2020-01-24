import { Storage } from '@google-cloud/storage';
import catchify from 'catchify';
import uuidV4 from 'uuid/v4';
import readFile from './readFile';
import writeToFile from './writeToFile';

const TWO_HOURS_MS = 1000 * 60 * 60 * 2;

const wait = (time: number) => new Promise((res) => setTimeout(() => res(), time));

const uuid = uuidV4();

const createLockFingerprint = () => `${uuid}:${new Date().getTime()}`;

const init = async (
  projectId: string,
  bucketName: string,
  folderName = 'gcr-persist',
  maxLockTime = TWO_HOURS_MS,
  waitTime = 2 * 1000,
) => {
  const bucket = new Storage({
    projectId,
  }).bucket(bucketName);

  const [bucketExistsErr, bucketExists] = await catchify(bucket.exists());

  if (bucketExistsErr) {
    throw bucketExistsErr;
  }

  if (!bucketExists) {
    throw new Error(`Bucket '${bucketName}' does not exist in project ${projectId}`);
  }

  return {
    get: async (key: string, ignoreLockfile = false) => {
      if (!ignoreLockfile) {
        const lockFile = bucket.file(`${folderName}/${key}.lock`);

        let [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

        let lockFileDeleted = false;

        if (lockFileExistsErr) {
          throw new Error('Error checking if lock file already exists.');
        }

        if (lockFileExists) {
          const [lockFileReadErr, lockFileContents] = await catchify(readFile(lockFile));

          if (lockFileReadErr) {
            throw new Error('Error checking if the lock file was too old.');
          }

          const lockFileContentsParts = lockFileContents.split(':');
          let shouldDelete = false;

          if (lockFileContentsParts.length !== 2) {
            console.log(`Lock file for key '${key}' corrupted, deleting.`);
            shouldDelete = true;
          } else {
            const lockFileTimestamp = new Date(lockFileContentsParts[1]);
            if (
              lockFileTimestamp.getTime() < new Date().getTime() - maxLockTime ||
              lockFileContentsParts[0] === uuid
            ) {
              // Lock file has existed for too long or it is our lock file
              shouldDelete = true;
            }
          }

          if (shouldDelete) {
            const [lockFileDeleteErr] = await catchify(lockFile.delete());

            if (lockFileDeleteErr) {
              throw lockFileDeleteErr;
            }

            lockFileDeleted = true;
          }
        }

        if (lockFileExists && !lockFileDeleted) {
          // lock file is there, lets try again after the wait time
          await wait(waitTime);

          [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

          if (lockFileExistsErr) {
            throw new Error('Error checking if lock file already exists.');
          }

          if (lockFileExists) {
            // lock file is there, lets try again after the wait time
            await wait(waitTime);

            [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

            if (lockFileExistsErr) {
              throw new Error('Error checking if lock file already exists.');
            }

            if (lockFileExists) {
              // lock file still there after 3 tries, give up
              throw new Error('File is currently locked.');
            }
          }
        }

        // Lock file is not there
        const [lockFileWriteErr] = await catchify(
          writeToFile(lockFile, createLockFingerprint(), 'text/plain'),
        );

        if (lockFileWriteErr) {
          throw lockFileWriteErr;
        }
      }

      const file = bucket.file(`${folderName}/${key}.json`);

      const [readFileErr, fileContents] = await catchify(readFile(file));

      if (readFileErr) {
        throw readFileErr;
      }

      return JSON.parse(fileContents);
    },
    save: async (key: string, objectToSave: object, ignoreLockfile = false) => {
      if (!ignoreLockfile) {
        const lockFile = bucket.file(`${folderName}/${key}.lock`);

        let [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

        if (lockFileExistsErr) {
          throw new Error('Error checking if lock file already exists.');
        }

        if (lockFileExists) {
          const [lockFileReadErr, lockFileContents] = await catchify(readFile(lockFile));

          if (lockFileReadErr) {
            throw new Error('Error checking the lock file.');
          }

          const lockFileContentsParts = lockFileContents.split(':');
          if (lockFileContentsParts !== 2) {
            throw new Error('Lock file is corrupted.');
          }
          if (lockFileContentsParts[0] !== uuid) {
            // this is not our lock file, lets try to wait and see if the lock file goes away
            await wait(waitTime);

            [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

            if (lockFileExistsErr) {
              throw new Error('Error checking if lock file exists.');
            }

            if (lockFileExists) {
              // lock file is still there, lets try again after the wait time
              await wait(waitTime);

              [lockFileExistsErr, lockFileExists] = await catchify(lockFile.exists());

              if (lockFileExistsErr) {
                throw new Error('Error checking if lock file exists.');
              }

              if (lockFileExists) {
                // lock file still there after 3 tries, give up
                throw new Error('File is currently locked.');
              }
            }
          }
        }
      }

      const file = bucket.file(`${folderName}/${key}.json`);

      const [writeFileErr] = await catchify(
        writeToFile(file, JSON.stringify(objectToSave), 'application/json'),
      );

      if (writeFileErr) {
        throw writeFileErr;
      }
    },
  };
};

export default init;
