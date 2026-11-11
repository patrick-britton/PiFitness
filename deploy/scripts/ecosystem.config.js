const testDir = process.env.TEST_DIR || '/home/god/PiFitness-test';
const port = process.env.PORT || '3000';

module.exports = {
  apps: [
    {
      name: 'pifitness-next-test',
      cwd: testDir + '/frontend/pifitness',
      script: 'node_modules/.bin/next',
      args: 'start -p ' + port + ' -H 127.0.0.1',
      env: {
        NODE_ENV: 'production',
        PORT: port,
        NEXT_PUBLIC_APP_ENV: 'test',
      },
      autorestart: true,
      max_memory_restart: '512M',
    },
  ],
};