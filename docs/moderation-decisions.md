# Moderation decisions

When a listing, a shop or a review is restricted, its owner receives a statement of reasons. A restriction itself is the moderator's tag in the marketplace index: the index hides the listing, shop or review, and this service refuses checkout, bids and offers on a moderated listing (`listing_moderation.rs`). The statement is recorded here, by the moderator who placed the tag, at the time of the decision.

The statement carries what the EU Digital Services Act (Regulation (EU) 2022/2065) Art. 17(3) requires: what the decision does, its territorial scope and duration, the facts relied on and whether it followed a notice or the moderators' own review, any use of automated means, the legal or Terms ground with an explanation, and (in the Shop) the redress options.

## Recording a decision

```text
DATABASE_URL=… MODERATION_ADMIN_OPERATOR=<operator id> moderation-decision-admin record decision.json
DATABASE_URL=… moderation-decision-admin show <decision id>
```

`record` validates the file, writes the decision, its event (`moderation.decision_recorded` on aggregate `moderation_decision:<id>`) and a `moderation_decision` notification intent to the owner of the target in one transaction, and prints the decision id. The id is the reference the owner quotes in an appeal. It is derived from the statement, so running `record` again on the same file (for example after a lost connection) prints the same id with "already recorded" and sends no second notice; a changed statement is a new decision.

Record the decision when the tag is placed, never later: the owner must have the statement from the moment the restriction applies. The notice goes to the owner named in `target_uri`, so copy the URI from the tag you placed and check it before running `record`; a decision is never edited or withdrawn once recorded.

`decision.json` (unknown fields are refused):

```json
{
  "restriction": "listing_removed",
  "target_uri": "pubky://<seller>/pub/pubky.app/marketplace/v1/listings/<listing id>",
  "territorial_scope": "Every region where Pubky Market is offered",
  "ends_at": null,
  "decided_at": "2026-11-04T12:00:00Z",
  "basis": "notice",
  "facts": "What was found, and what it was found in.",
  "automated_detection": false,
  "automated_decision": false,
  "automated_means": null,
  "ground": "terms",
  "ground_reference": "Pubky Terms of Service, Prohibited goods",
  "explanation": "Why the content breaks that ground."
}
```

| Field | Values |
| --- | --- |
| `restriction` | `listing_removed` (out of the catalogue; cannot be bought, bid on or offered on), `shop_suspended` (the seller's shop and every listing), `review_text_hidden` (text hidden, rating kept), `review_excluded` (hidden and left out of every rating) |
| `target_uri` | `listing_removed`: `pubky://<seller>/pub/pubky.app/marketplace/v1/listings/<id>`. `shop_suspended`: `pubky://<seller>/pub/pubky.app/profile.json`. Review restrictions: `pubky://<reviewer>/pub/pubky.app/marketplace/v1/reviews/<id>`. The owner in the URI receives the notice. |
| `territorial_scope` | Where the restriction applies, in words (1–200 characters). |
| `ends_at` | `null` until lifted, or an RFC 3339 time after `decided_at`. |
| `decided_at` | When the decision was taken; at most five minutes ahead of the operator's clock (skew). |
| `basis` | `notice` (a report under the Complaint Procedure) or `own_initiative`. |
| `facts` | The facts and circumstances relied on (1–2000 characters). Name a reporter only where strictly necessary. |
| `automated_detection`, `automated_decision` | Whether automated means detected the content, and whether they took the decision. |
| `automated_means` | Required when either flag is true, and only then: what the automated means were and what they did (1–1000 characters). |
| `ground` | `illegal_content` or `terms`. |
| `ground_reference` | The law relied on, or the Terms clause (1–500 characters). |
| `explanation` | Why the content is illegal on that ground, or breaks that clause (1–2000 characters). |

`MODERATION_ADMIN_OPERATOR` is stored with the decision for the record and the appeal review; it is never shown to the owner.

## What the owner sees

`GET /v1/notifications` returns the decision on the `moderation_decision` notification as `moderation_decision`, to its recipient only. Every other notification has `moderation_decision: null`. Decisions are append-only: a decision is never edited or deleted. Lifting a restriction (for example after an upheld appeal) is the moderator removing the tag; the decision stays as the record.
