// create-order : reçoit la commande (Function URL), valide, publie dans SQS, répond 202.
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import type {
  APIGatewayProxyEventV2,
  APIGatewayProxyResultV2,
} from "aws-lambda";

// Astuce : le client est créé HORS du handler, il est réutilisé entre les invocations.
// L'endpoint LocalStack est lu automatiquement dans AWS_ENDPOINT_URL quand il est défini.
const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL!;

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

function reply(
  statusCode: number,
  body: unknown,
  correlationId?: string,
): APIGatewayProxyResultV2 {
  return {
    statusCode,
    headers: {
      "content-type": "application/json",
      ...(correlationId ? { "x-correlation-id": correlationId } : {}),
    },
    body: JSON.stringify(body),
  };
}

export const handler = async (
  event: APIGatewayProxyEventV2,
): Promise<APIGatewayProxyResultV2> => {
  // L'identifiant de corrélation suit la commande de bout en bout (journaux).
  const correlationId = event.headers?.["x-correlation-id"] ?? randomUUID();

  // TODO 1 : lire l'en-tête « idempotency-key » (les en-têtes arrivent en minuscules). S'il manque : répondre 400.
  const idempotencyKey = event.headers?.["idempotency-key"]?.trim();
  if (!idempotencyKey) {
    return reply(
      400,
      { error: "missing Idempotency-Key header" },
      correlationId,
    );
  }

  // TODO 2 : décoder le corps (JSON, éventuellement en base64 : event.isBase64Encoded) et valider : eventId (texte non vide), seats (entier de 1 à 6), email (EMAIL_RE). Corps invalide : répondre 400 avec la liste des erreurs.
  let body: any;
  try {
    const raw = event.isBase64Encoded
      ? Buffer.from(event.body ?? "", "base64").toString("utf-8")
      : (event.body ?? "");
    body = JSON.parse(raw);
  } catch {
    return reply(400, { error: "invalid JSON" }, correlationId);
  }

  // Un JSON valide n'est pas forcément un objet (null, 42, [] ...)
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return reply(400, { error: "body must be a JSON object" }, correlationId);
  }

  const errors: string[] = [];
  if (typeof body.eventId !== "string" || body.eventId.trim() === "") {
    errors.push("eventId is required and must be a non-empty string");
  }
  if (!Number.isInteger(body.seats) || body.seats < 1 || body.seats > 6) {
    errors.push("seats is required and must be an integer between 1 and 6");
  }
  if (typeof body.email !== "string" || !EMAIL_RE.test(body.email)) {
    errors.push("email is required and must be a valid email address");
  }

  if (errors.length > 0) {
    return reply(
      400,
      { error: "validation failed", details: errors },
      correlationId,
    );
  }

  // TODO 3 : fabriquer le message {orderId, correlationId, idempotencyKey, eventId, seats, email, createdAt} et l'envoyer avec SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: ... }).
  const message = {
    orderId: randomUUID(),
    correlationId,
    idempotencyKey,
    eventId: body.eventId,
    seats: body.seats,
    email: body.email,
    createdAt: new Date().toISOString(),
  };

  try {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify(message),
      }),
    );
  } catch (err) {
    console.log(
      JSON.stringify({
        level: "error",
        msg: "sqs send failed",
        correlationId,
        orderId: message.orderId,
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    return reply(500, { error: "could not enqueue order" }, correlationId);
  }

  // TODO 4 : journaliser UNE ligne JSON {level, msg, correlationId, orderId} avec console.log.
  console.log(
    JSON.stringify({
      level: "info",
      msg: "order accepted",
      correlationId,
      orderId: message.orderId,
    }),
  );

  // TODO 5 : répondre 202 avec { orderId, correlationId }.
  return reply(202, { orderId: message.orderId, correlationId }, correlationId);
};
