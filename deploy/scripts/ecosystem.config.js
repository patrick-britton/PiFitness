module.exports = {
  apps: [
    {
      name: 'pifitness-next-test',
      cwd: '/home/god/PiFitness-test/frontend/pifitness',
      script: 'node_modules/.bin/next',
      args: 'start -p 3001',
      env: {
        NODE_ENV: 'production',
        PORT: '3001',
        NEXT_PUBLIC_APP_ENV: 'test',
      },
      autorestart: true,
      max_memory_restart: '512M',
    },
  ],
};