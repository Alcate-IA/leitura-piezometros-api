require('dotenv').config(); 
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const Firebird = require('node-firebird');
const iconv = require('iconv-lite'); 

// --- CONFIGURAÇÕES ---
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://n8n.alcateia-ia.com/webhook-test/leituras';
const FOTOS_PATH = process.env.FOTOS_PATH || path.join(__dirname, 'fotos-inspecoes');
const MQTT_TOPIC_BASE = process.env.MQTT_TOPIC_RESULTADO || 'alcateia/teste/riodeserto/lista/piezometro'; 
const INTERVALO_CONSULTA = process.env.INTERVALO_CONSULTA_MS || 60000;

// Configuração do Banco
const dbOptions = {
    host: process.env.DB_HOST || '192.9.200.7',
    port: process.env.DB_PORT || 3050,
    database: process.env.DB_DATABASE || '/data1/dataib/zeus20.fdb',
    user: process.env.DB_USER || 'ALCATEIA',
    password: process.env.DB_PASSWORD || '8D5Z9s2F',
    lowercase_keys: false, 
    role: null,            
    pageSize: 4096,
    charset: 'NONE' // Mantemos NONE para receber o Buffer do CAST
};

// Configuração MQTT
const mqttOptions = {
    host: process.env.MQTT_HOST || 'broker.hivemq.com',
    port: process.env.MQTT_PORT || 8883,
    protocol: 'mqtts',
    username: process.env.MQTT_USER || '',
    password: process.env.MQTT_PASSWORD || '',
    connectTimeout: 10000,
    rejectUnauthorized: false 
};

// --- FUNÇÃO DE DECODIFICAÇÃO ---
function decodificarBuffer(valor) {
    if (!valor) return null;

    if (Buffer.isBuffer(valor)) {
        return iconv.decode(valor, 'win1252').trim(); 
    }
    
    if (typeof valor === 'string') return valor.trim();
    
    return valor;
}

// --- CONFIGURAÇÕES BÁSICAS ---
if (!WEBHOOK_URL) process.exit(1);
if (!fs.existsSync(FOTOS_PATH)) fs.mkdirSync(FOTOS_PATH, { recursive: true });

let bufferLeituras = null;
const bufferFotos = new Map();

// --- CONEXÃO MQTT ---
console.log(`🔌 Conectando ao MQTT...`);
const client = mqtt.connect(mqttOptions);

client.on('connect', () => {
    console.log('✅ Conectado ao HiveMQ (SSL)');
    client.subscribe('alcateia/teste/riodeserto/emcampo/leituras');
    client.subscribe('alcateia/teste/riodeserto/emcampo/fotos/#');

    // Inicia o ciclo de banco
    setInterval(consultarBancoPublicarMQTT, INTERVALO_CONSULTA);
    consultarBancoPublicarMQTT();
});

client.on('message', (topic, message) => {
    if (topic.startsWith(MQTT_TOPIC_BASE)) return;
    try {
        const payload = JSON.parse(message.toString());
        if (topic.includes('leituras')) { 
            console.log('📥 Recebido pacote de leituras. Iniciando espera por fotos...');
            bufferLeituras = payload; 
            reiniciarTimeout(); 
        }
        else if (topic.includes('fotos')) { 
            const id = topic.split('/').pop();
            bufferFotos.set(id, payload.fotoBase64); 
            reiniciarTimeout(); 
        }
    } catch (e) { console.error('Erro parse JSON:', e.message); }
});

// --- LÓGICA CONCILIAÇÃO ---
let timeoutHandle = null;
function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) return;
    
    console.log('🔄 Processando dados para envio ao Webhook...');
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo);

    categorias.forEach(cat => {
        if (campo[cat]) {
            campo[cat] = campo[cat].map(leitura => {
                let caminhoFotoFinal = null;
                if (bufferFotos.has(leitura.id)) {
                    const base64Data = bufferFotos.get(leitura.id);
                    const codigoPonto = leitura.poco ? leitura.poco.split(' - ')[0].trim() : 'NA';
                    const nomeArquivo = `${codigoPonto} - ${leitura.id}.jpg`;
                    const caminhoCompleto = path.join(path.resolve(FOTOS_PATH), nomeArquivo);
                    try {
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        bufferFotos.delete(leitura.id);
                        caminhoFotoFinal = caminhoCompleto;
                    } catch (err) { console.error('Erro salvar foto:', err.message); }
                }
                const infoAdicional = {
                    comentario: leitura.observacao && leitura.observacao.trim() !== "" ? leitura.observacao : null,
                    url: caminhoFotoFinal 
                };
                return { ...leitura, observacao: JSON.stringify(infoAdicional) };
            });
        }
    });

    try {
        console.log(`🚀 Enviando POST para: ${WEBHOOK_URL}`);
        
        // Envio com axios e melhor tratamento de erro
        const response = await axios.post(WEBHOOK_URL, bufferLeituras, {
            headers: { 'Content-Type': 'application/json' }
        });
        
        console.log(`✅ Sucesso! Status: ${response.status}`);
        bufferLeituras = null;
    } catch (error) { 
        if (error.response) {
            // O servidor respondeu com um status fora de 2xx
            console.error(`❌ Erro HTTP ${error.response.status} no Webhook:`);
            console.error(`   Dados:`, JSON.stringify(error.response.data));
            console.error(`   URL Tentada: ${WEBHOOK_URL}`);
        } else if (error.request) {
            // A requisição foi feita mas não houve resposta
            console.error('❌ Sem resposta do Webhook (Timeout ou Rede indisponível).');
        } else {
            console.error('❌ Erro na configuração do Axios:', error.message);
        }
    }
}

// --- BANCO DE DADOS ---
function consultarBancoPublicarMQTT() {
    const sqlQuery = `
        SELECT 
            P.CD_PIEZOMETRO, 
            CAST(P.ID_PIEZOMETRO AS VARCHAR(200) CHARACTER SET OCTETS) AS ID_RAW,
            CAST(P.NM_PIEZOMETRO AS VARCHAR(1024) CHARACTER SET OCTETS) AS NM_RAW,
            CAST(P.TP_PIEZOMETRO AS VARCHAR(50) CHARACTER SET OCTETS) AS TP_RAW,
            CAST(P.FG_SITUACAO AS VARCHAR(20) CHARACTER SET OCTETS) AS FG_RAW,
            P.CD_EMPRESA,
            MAX(I.DT_INSPECAO) AS DT_INSPECAO,
            MAX(I.QT_LEITURA) AS QT_LEITURA
        FROM 
            TB_PIEZOMETRO P
        LEFT JOIN 
            TB_INSPECAO_PIEZOMETRO_MVTO I 
            ON I.CD_INSPECAO_PIEZOMETRO = P.CD_PIEZOMETRO
            AND I.DT_INSPECAO = (
                SELECT MAX(SUB.DT_INSPECAO)
                FROM TB_INSPECAO_PIEZOMETRO_MVTO SUB
                WHERE SUB.CD_INSPECAO_PIEZOMETRO = P.CD_PIEZOMETRO
            )
        WHERE 
            P.FG_SITUACAO = 'A' 
            AND P.CD_EMPRESA = '18'
        GROUP BY 
            P.CD_PIEZOMETRO, 
            P.ID_PIEZOMETRO, 
            P.NM_PIEZOMETRO,
            P.TP_PIEZOMETRO,
            P.FG_SITUACAO, 
            P.CD_EMPRESA
    `;

    Firebird.attach(dbOptions, (err, db) => {
        if (err) { console.error('Erro Firebird:', err.message); return; }

        db.query(sqlQuery, (err, result) => {
            db.detach(); 

            if (err) { console.error('Erro Query:', err.message); return; }

            if (result && result.length > 0) {
                const dadosAgrupados = {};

                result.forEach(row => {
                    const idDecodificado = decodificarBuffer(row.ID_RAW);
                    const nmDecodificado = decodificarBuffer(row.NM_RAW);
                    const tpDecodificado = decodificarBuffer(row.TP_RAW);
                    const fgDecodificado = decodificarBuffer(row.FG_RAW);

                    const objetoLimpo = {
                        CD_PIEZOMETRO: row.CD_PIEZOMETRO,
                        ID_PIEZOMETRO: idDecodificado,
                        NM_PIEZOMETRO: nmDecodificado,
                        TP_PIEZOMETRO: tpDecodificado,
                        CD_EMPRESA: row.CD_EMPRESA,
                        FG_SITUACAO: fgDecodificado,
                        DT_INSPECAO: row.DT_INSPECAO,
                        QT_LEITURA: row.QT_LEITURA
                    };

                    const tipo = tpDecodificado || 'OUTROS';
                    if (!dadosAgrupados[tipo]) dadosAgrupados[tipo] = [];
                    dadosAgrupados[tipo].push(objetoLimpo);
                });

                Object.keys(dadosAgrupados).forEach(tipo => {
                    const topico = `${MQTT_TOPIC_BASE}/${tipo}`;
                    client.publish(topico, JSON.stringify(dadosAgrupados[tipo]), { retain: true, qos: 1 });
                    console.log(`📡 Publicado: ${topico} (${dadosAgrupados[tipo].length} itens)`);
                });
            } else {
                console.log('⚠️ Consulta vazia.');
            }
        });
    });
}