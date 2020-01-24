import { File } from '@google-cloud/storage';

export default (file: File, strToWrite: string, contentType) => {
  return file.save(strToWrite, {
    contentType,
  });
};
