'use strict';
const http=require('http');
const WebSocket=require('ws');
const crypto=require('crypto');
const PORT=process.env.PORT||3000;
const MSG_TTL_MS=24*60*60*1000;
const MAX_MSG_LEN=100000;
const MAX_ROOM_MSGS=200;
const rooms=new Map();
setInterval(()=>{const now=Date.now();for(const[roomId,room]of rooms.entries()){room.messages=room.messages.filter(m=>now-m.ts<MSG_TTL_MS);if(room.clients.size===0&&room.messages.length===0)rooms.delete(roomId);}},60000);
function getOrCreateRoom(r){if(!rooms.has(r))rooms.set(r,{clients:new Set(),messages:[]});return rooms.get(r);}
function broadcast(room,data,exclude){const p=JSON.stringify(data);for(const c of room.clients)if(c!==exclude&&c.readyState===WebSocket.OPEN)c.send(p);}
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'text/plain'});res.end('OK');});
const wss=new WebSocket.Server({server});
wss.on('connection',(ws)=>{
ws._roomId=null;
ws.on('message',(raw)=>{
let msg;try{if(raw.length>MAX_MSG_LEN)return;msg=JSON.parse(raw);}catch{return;}
if(msg.type==='join'){const roomId=String(msg.room||'').slice(0,64).replace(/[^a-zA-Z0-9_-]/g,'');if(!roomId)return;ws._roomId=roomId;const room=getOrCreateRoom(roomId);room.clients.add(ws);ws.send(JSON.stringify({type:'history',messages:room.messages}));broadcast(room,{type:'online',count:room.clients.size},ws);ws.send(JSON.stringify({type:'online',count:room.clients.size}));}
else if(msg.type==='message'){if(!ws._roomId)return;const room=rooms.get(ws._roomId);if(!room)return;const m={id:crypto.randomUUID(),cipher:String(msg.cipher||''),sid:String(msg.sid||''),ts:Date.now()};room.messages.push(m);if(room.messages.length>MAX_ROOM_MSGS)room.messages.shift();broadcast(room,{type:'message',...m},null);}
else if(msg.type==='delete'){if(!ws._roomId)return;const room=rooms.get(ws._roomId);if(!room)return;room.messages=room.messages.filter(m=>m.id!==msg.id);broadcast(room,{type:'deleted',id:msg.id},null);}
});
ws.on('close',()=>{if(!ws._roomId)return;const room=rooms.get(ws._roomId);if(!room)return;room.clients.delete(ws);broadcast(room,{type:'online',count:room.clients.size},null);});
ws.on('error',()=>{});
});
server.listen(PORT,()=>console.log('OK '+PORT));
