const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder, PermissionFlagsBits } = require('discord.js');
const { WebSocketServer } = require('ws');
const express = require('express');
const http = require('http');
const https = require('https');
const os = require('os');
const fsSync = require('fs');

// Télécharge un fichier Discord en mémoire et le re-sert via une URL temporaire locale
const fileCache = new Map(); // id -> { buffer, contentType, expires }

function downloadBuffer(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ buffer: Buffer.concat(chunks), contentType: res.headers['content-type'] || '' }));
      res.on('error', reject);
    }).on('error', reject);
  });
}

const TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const PORT = process.env.PORT || 3000;
const ROLE_NAME = 'Tars';

if (!TOKEN || !CLIENT_ID) {
  console.error('Manque DISCORD_TOKEN ou CLIENT_ID');
  process.exit(1);
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.get('/', (req, res) => res.send('LiveChat Server OK'));
app.get('/health', (req, res) => res.json({ status: 'ok', clients: getTotalClients() }));

app.get('/media/:id', (req, res) => {
  const cached = fileCache.get(req.params.id);
  if (!cached) { res.status(404).send('Not found'); return; }
  res.setHeader('Content-Type', cached.contentType);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.send(cached.buffer);
});

// clients : Map<guildId, Set<ws>>
const clients = new Map();

function getTotalClients() {
  let n = 0;
  for (const s of clients.values()) n += s.size;
  return n;
}

wss.on('connection', (ws) => {
  let guildId = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'register' && msg.guildId) {
        guildId = msg.guildId;
        if (!clients.has(guildId)) clients.set(guildId, new Set());
        clients.get(guildId).add(ws);
        ws.send(JSON.stringify({ type: 'registered' }));
        console.log(`[WS] +1 client guild ${guildId} (total: ${clients.get(guildId).size})`);
      }
    } catch {}
  });

  ws.on('close', () => {
    if (guildId && clients.has(guildId)) {
      clients.get(guildId).delete(ws);
      if (clients.get(guildId).size === 0) clients.delete(guildId);
    }
  });

  ws.on('error', () => {});
});

function broadcast(guildId, payload) {
  const guild = clients.get(guildId);
  if (!guild || guild.size === 0) return 0;
  const data = JSON.stringify(payload);
  let sent = 0;
  for (const ws of guild) {
    if (ws.readyState === ws.OPEN) { ws.send(data); sent++; }
  }
  return sent;
}

// Bot Discord
const bot = new Client({ intents: [GatewayIntentBits.Guilds] });

bot.once('clientReady', async () => {
  console.log(`[Bot] Connecté : ${bot.user.tag}`);
  await registerCommands();
});

bot.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand() || interaction.commandName !== 'livechat') return;

  // Vérif rôle Tars
  const member = interaction.member;
  const hasRole = member.roles.cache.some(r => r.name === ROLE_NAME);
  if (!hasRole) {
    await interaction.reply({ content: `❌ Tu dois avoir le rôle **${ROLE_NAME}** pour utiliser cette commande.`, ephemeral: true });
    return;
  }

  // Vérif guild
  const guildId = interaction.guildId;
  const connected = clients.get(guildId)?.size || 0;

  // File d'attente côté serveur par guild
  if (!queues.has(guildId)) queues.set(guildId, []);
  const queue = queues.get(guildId);

  if (queue.length >= 3) {
    await interaction.reply({ content: '⏳ File d\'attente pleine (3/3), réessaie dans quelques secondes.', ephemeral: true });
    return;
  }

  const text = interaction.options.getString('texte') || '';
  const attachment = interaction.options.getAttachment('media');

  if (!text && !attachment) {
    await interaction.reply({ content: '❌ Donne un texte ou un média.', ephemeral: true });
    return;
  }

  let url = null;
  let mediaType = null;

  if (attachment) {
    const ct = attachment.contentType || '';
    if (ct.startsWith('video/')) mediaType = 'video';
    else if (ct.includes('gif')) mediaType = 'gif';
    else if (ct.startsWith('audio/') || /\.(mp3|wav|ogg|aac|flac)$/i.test(attachment.name || '')) mediaType = 'audio';
    else mediaType = 'image';

    try {
      const { buffer, contentType } = await downloadBuffer(attachment.url);
      const id = Date.now().toString(36) + Math.random().toString(36).slice(2);
      fileCache.set(id, { buffer, contentType: contentType || ct });
      // Expire après 1 minute
      setTimeout(() => fileCache.delete(id), 60000);
      // URL locale servie par Express
      const host = process.env.RAILWAY_PUBLIC_DOMAIN
        ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`
        : `http://localhost:${PORT}`;
      url = `${host}/media/${id}`;
    } catch(e) {
      url = attachment.url; // fallback lien Discord
      console.error('[Media] Erreur download:', e.message);
    }
  }

  const payload = {
    type: 'media',
    username: interaction.member.displayName || interaction.user.username,
    avatar: interaction.user.displayAvatarURL({ size: 64 }),
    text,
    url,
    mediaType,
  };

  queue.push(payload);
  processQueue(guildId);

  await interaction.reply({
    content: `✅ Envoyé à **${connected}** écran${connected !== 1 ? 's' : ''} !`,
    ephemeral: true
  });
});

// File d'attente par guild
const queues = new Map();
const processing = new Set();

function processQueue(guildId) {
  if (processing.has(guildId)) return;
  const queue = queues.get(guildId);
  if (!queue || queue.length === 0) return;

  processing.add(guildId);
  const payload = queue[0];

  broadcast(guildId, { ...payload, action: 'show' });

  // Durée : le serveur envoie "done" après le délai max
  // Le client gère sa propre durée, le serveur dépile après 35s max
  setTimeout(() => {
    queue.shift();
    processing.delete(guildId);
    if (queue.length > 0) processQueue(guildId);
  }, 35000);
}

async function registerCommands() {
  const cmd = new SlashCommandBuilder()
    .setName('livechat')
    .setDescription('Affiche un média sur tous les écrans connectés')
    .addStringOption(o => o.setName('texte').setDescription('Message à afficher').setRequired(false))
    .addAttachmentOption(o => o.setName('media').setDescription('Image, GIF ou vidéo').setRequired(false));

  const rest = new REST({ version: '10' }).setToken(TOKEN);
  try {
    await rest.put(Routes.applicationCommands(CLIENT_ID), { body: [cmd.toJSON()] });
    console.log('[Bot] Commande /livechat enregistrée globalement');
  } catch (e) {
    console.error('[Bot] Erreur commande:', e.message);
  }
}

bot.login(TOKEN);
server.listen(PORT, () => console.log(`[Server] Port ${PORT}`));
