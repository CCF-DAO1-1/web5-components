import express, { Request, Response } from 'express';
import { ccc } from '@ckb-ccc/core';

const app = express();
const port = 3000;
const cccClient = new ccc.ClientPublicTestnet();

app.use(express.json());

interface QueryParams {
  msg?: string;
  sig?: string;
}

app.get('/recover', async (req: Request<{}, {}, {}, QueryParams>, res: Response) => {
  try {
    const { msg, sig } = req.query;

    if (!msg || !sig || typeof msg !== 'string' || typeof sig !== 'string') {
      return res.status(400).json({ error: 'Invalid parameters. Both msg and sig must be strings.' });
    }
    console.log("msg: ", msg);
    console.log("sig: ", sig);
  
    // Convert sig from hex string to Uint8Array
    // remove 0x prefix if exists
    const sigBytes = ccc.bytesFrom(sig.replace("0x", ""), "hex");
    const decoder = new TextDecoder();
    const sigJson = decoder.decode(sigBytes);
    console.log(sigJson);

    // Parse signature from JSON string
    const sigObj = JSON.parse(sigJson);
    
    // Recover signer from signature
    const fromSigner = await ccc.signerFromSignature(cccClient, sigObj, msg);
    if (!fromSigner) {
      return res.status(400).json({ error: 'Failed to recover signer from signature' });
    }

    const isVerified = await fromSigner.verifyMessage(msg, sigObj);
    console.log("is verified: ", isVerified);
    if (!isVerified) {
      return res.status(400).json({ error: 'Failed to verify message with signature' });
    }

    // Get recommended address from signer
    const fromAddr = await fromSigner.getRecommendedAddress();
    if (!fromAddr) {
      return res.status(400).json({ error: 'Failed to get address from signer' });
    }

    res.json({ address: fromAddr.toString() });
  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});