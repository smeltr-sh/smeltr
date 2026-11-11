const { chromium } = require('playwright'); const fs=require('fs'); const path=process.argv[2];
(async()=>{const b=await chromium.launch();const p=await b.newPage({viewport:{width:760,height:740},deviceScaleFactor:2});
await p.setContent(`<body style="margin:0;background:#fff">${fs.readFileSync(path,'utf8')}</body>`);await p.screenshot({path:path.replace('.svg','.png')});await b.close();})();
