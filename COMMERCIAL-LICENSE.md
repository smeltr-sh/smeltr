# Smeltr Commercial License

Version 1.0. Published at [smeltr.sh/license/commercial](https://smeltr.sh/license/commercial).

## 1. Parties

This license is between John Pavlik (the "Licensor") and the organization named on the invoice, together with every organization it controls (the "Customer"). Control means owning more than half of an organization or directing its management, directly or through others.

## 2. What this license adds

Smeltr is published under the Business Source License 1.1 (the "BSL"), the [LICENSE](https://github.com/smeltr-sh/smeltr/blob/main/LICENSE) file in its repository. The BSL lets anyone copy, modify and redistribute Smeltr and use it for evaluation, development and testing, and it lets an individual use it in production for themselves, and anyone use it in production on behalf of an organization with fewer than 250 employees. Whoever the BSL does not allow production use must buy a commercial license. This is that license.

This license adds production use and support, and nothing else. Every other term of the BSL still applies, including its disclaimer of warranties.

## 3. Definitions

- A **Leader** is one running Smeltr process in the leader role, with its own data directory. Restarting a Leader, or moving it to another machine with its data directory, does not make it a new Leader. Workers are Smeltr processes that take their deployments from a Leader; they are not counted.
- A Leader is **in production** when the data flowing through it or through its workers serves the Customer's operations, or those of the Customer's own customers. A Leader used only to evaluate, develop or test Smeltr is not in production, and the BSL already allows it.
- A **License** is one paid invoice line for one Leader. It runs for one year from its purchase date. Each License has its own term; buying another does not move the term of the first.

## 4. Grant

The Licensor grants the Customer a non-exclusive right, transferable only as section 14 allows, to run Smeltr in production, with at any one time no more Leaders in production than the Customer has Licenses in force. Each Leader may serve any number of workers.

The grant covers every release of Smeltr, with or without the Customer's own modifications, including releases the Licensor publishes while a License is in force.

## 5. What is not granted

This license does not allow the Customer to offer Smeltr to third parties as a hosted or managed service, or to resell or sublicense it. Each of those needs a separate written agreement with the Licensor. This license grants no right in the Licensor's trademarks or logos.

## 6. Fee, proof and refund

The fee is USD 10,000 per License, paid in advance. Fees exclude taxes. The Customer accepts this license by paying the invoice. The paid invoice is the proof of license: it names the Customer, the number of Licenses, their purchase date and the version of this license. Smeltr has no license key and nothing in it needs activating.

The Licensor refunds a License in full if the Customer asks within 30 days of its purchase date, for any reason or none. The refunded License ends on refund.

## 7. Term and renewal

A License lasts one year from its purchase date. It renews only when the Customer buys a License for the next year; nothing renews or is charged by itself.

The Customer may run a Leader in production beyond its Licenses in force for up to 30 days, provided it buys a License for that Leader within those 30 days. The added License runs one year from its own purchase date. Past the 30 days, the extra Leader is a breach of this license.

## 8. Support

The Licensor answers the Customer's support questions by email at support@smeltr.sh. The Licensor does not promise a response time, a fix or a release date.

## 9. Ending

A License ends when its year ends without renewal, or when it is refunded. The whole grant ends 30 days after the Licensor gives notice of a breach of this license that the Customer has not cured. When a Customer has no License in force, the Customer stops production use that the BSL does not allow on its own. Releases that have passed their BSL Change Date are licensed under the Apache License 2.0 and are not affected.

## 10. Liability

Smeltr is provided "as is", as the BSL says. To the extent the law allows, the Licensor's total liability under this license is limited to the fees the Customer paid under it in the twelve months before the claim, and the Licensor is not liable for lost profits, lost data or other indirect damages.

## 11. Governing law

This license is governed by the laws of the State of Ohio, USA, and disputes under it are heard in the courts of Ohio.

## 12. Versions of this license

The Licensor may publish a new version of this license at the address above. The version named on a License's invoice governs that License for its year; a new version applies to Licenses bought after it is published.

## 13. Whole agreement

This license, the BSL and the invoice are the whole agreement between the Licensor and the Customer about Smeltr, unless the two sign a separate written agreement.

## 14. General

- If a court finds a term of this license unenforceable, the rest stays in force.
- A right the Licensor does not enforce at once is not given up.
- Notices go by email: to the Customer at the address on the invoice, to the Licensor at support@smeltr.sh.
- The Customer may transfer this license only with the whole business that uses Smeltr, and tells the Licensor when it does.
