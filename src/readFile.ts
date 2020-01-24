import { File } from '@google-cloud/storage';

export default (file: File): Promise<string> =>
  new Promise((res, rej) => {
    let buffer = '';
    const readStream = file.createReadStream();
    readStream.on('data', (data) => {
      buffer += data;
    });
    readStream.on('end', () => {
      res(buffer);
    });
    readStream.on('error', (err) => {
      rej(err);
    });
  });
