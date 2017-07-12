- [ ] **Submitter**: Include the JIRA issue number in the PR description
- [ ] **Submitter**: Make sure Swagger is updated if API changes
- [ ] **Submitter**: If updating admin endpoints, also update [firecloud-admin-cli](https://github.com/broadinstitute/firecloud-admin-cli)
- [ ] **Submitter**: Check documentation and code comments. Add explanatory PR comments if helpful.
- [ ] **Submitter**: JIRA ticket checks:
  * Acceptance criteria exists and is met
  * Note any changes to implementation from the description
  * To Demo flag is set
  * Release Summary is filled out, if applicable
  * Add notes on how to QA
- [ ] **Submitter**: Update RC_XXX release ticket with any config or environment changes necessary
- [ ] **Submitter**: Update FISMA documentation if changes to:
  * Authentication
  * Authorization
  * Encryption
  * Audit trails
- [ ] Tell the tech lead (TL) that the PR exists if they wants to look at it
- [ ] Anoint a lead reviewer (LR). **Assign PR to LR**
* Review cycle:
  * LR reviews
  * Rest of team may comment on PR at will
  * **LR assigns to submitter** for feedback fixes
  * Submitter rebases to develop again if necessary
  * Submitter makes further commits. DO NOT SQUASH
  * Submitter updates documentation as needed
  * Submitter **reassigns to LR** for further feedback
- [ ] **TL** sign off
- [ ] **LR** sign off
- [ ] **Product Owner** sign off
- [ ] **Assign to submitter** to finalize
- [ ] **Submitter**: Verify all tests go green, including CI tests
- [ ] **Submitter**: Squash commits and merge to develop
- [ ] **Submitter**: Delete branch after merge
- [ ] **Submitter**: **Test this change works on dev environment after deployment**. YOU own getting it fixed if dev isn't working for ANY reason!
- [ ] **Submitter**: Verify swagger UI on dev environment still works after deployment
- [ ] **Submitter**: Inform other teams of any API changes via Slack and/or email
- [ ] **Submitter**: Mark JIRA issue as resolved once this checklist is completed
