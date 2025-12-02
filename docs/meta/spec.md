# Telegraph-Mailchimp — Package Specification

> **Cluster:** `integration`
> **Language:** `php`
> **Milestone:** `m5`
> **Repo:** `https://github.com/decodelabs/telegraph-mailchimp`
> **Role:** Mailchimp adapter for Telegraph

## Overview

### Purpose

Telegraph-Mailchimp provides a Mailchimp adapter implementation for the Telegraph mailing list manager. It implements the `Adapter` interface from Telegraph, allowing Telegraph to interact with Mailchimp's API for mailing list operations.

Key features:
- **Mailchimp API integration**: Full integration with Mailchimp Marketing API v3
- **List operations**: Fetching list information, groups, tags, and consent fields
- **Member operations**: Subscribe, update, and unsubscribe members
- **Group management**: Support for Mailchimp interest categories and interests (groups)
- **Tag management**: Support for Mailchimp tags
- **Consent fields**: Optional support for Mailchimp marketing permissions (consent fields)
- **Error handling**: Comprehensive error handling with failure reason mapping
- **Email hashing**: MD5 hashing for Mailchimp member lookups

### Non-Goals

- Telegraph-Mailchimp does not provide a full implementation of all Mailchimp API features.
- It does not handle campaign management or automation workflows.
- It does not provide analytics or reporting features.
- It does not handle webhook processing or event handling.
- It does not provide batch operations or bulk imports.

## Role in the Ecosystem

### Cluster & Positioning

Telegraph-Mailchimp belongs to the **integration** cluster, providing a specific adapter implementation for the Telegraph mailing list manager. It extends Telegraph's functionality by adding Mailchimp as a supported mailing list provider.

### Usage Contexts

- **Mailchimp integration**: Using Mailchimp as a mailing list provider via Telegraph
- **Newsletter subscriptions**: Managing Mailchimp newsletter subscriptions
- **Interest management**: Managing Mailchimp interest categories and interests
- **Tag management**: Managing Mailchimp tags for member segmentation
- **Consent management**: Managing Mailchimp marketing permissions (when enabled)

## Public Surface

### Key Types

- **`Adapter\Mailchimp`** (class): Main adapter class implementing the `Adapter` interface. Handles all Mailchimp API interactions.

- **`Adapter\Mailchimp\ListsApiOverride`** (class): Override class for Mailchimp ListsApi to fix tag search count limitation. Extends `MailchimpMarketing\Api\ListsApi`.

### Main Entry Points

**Mailchimp Adapter:**
- `new Mailchimp(array $settings)` — Constructor
  - `$settings['apiKey']` — Mailchimp API key (required)
  - `$settings['consent']` — Enable consent field support (optional, default: `false`)

**Adapter Interface Methods:**
- `$adapter->fetchAllListReferences(): array` — Fetch all list references
- `$adapter->fetchListInfo(SourceReference $source): ?ListInfo` — Fetch list information
- `$adapter->subscribe(SourceReference $source, ListInfo $listInfo, MemberDataRequest $request): AdapterActionResult` — Subscribe member
- `$adapter->update(SourceReference $source, ListInfo $listInfo, string $email, MemberDataRequest $request): AdapterActionResult` — Update member
- `$adapter->unsubscribe(SourceReference $source, ListInfo $listInfo, string $email): AdapterActionResult` — Unsubscribe member
- `$adapter->fetchMemberInfo(SourceReference $source, ListInfo $listInfo, string $email): ?MemberInfo` — Fetch member information

**Protected Methods:**
- `$adapter->updateMember(SourceReference $source, ListInfo $listInfo, string $email, MemberDataRequest $request, bool $subscribe = false): AdapterActionResult` — Internal update member method
- `$adapter->withListsApi(callable $action, ?SourceReference $source = null, ?MemberDataRequest $dataRequest = null, bool $nullOn404 = false): mixed` — Wrap API calls with error handling
- `$adapter->getApiClient(): ApiClient` — Get Mailchimp API client
- `$adapter->getListsApi(): ListsApi` — Get Lists API instance
- `$adapter->hashEmail(string $email): string` — Hash email for Mailchimp lookup
- `$adapter->normalizeStatus(string $status): MemberStatus` — Normalize Mailchimp status to MemberStatus enum
- `$adapter->getMergeName(?object $mergeFields): ?string` — Extract name from merge fields

## Dependencies

### Decode Labs

- **`decodelabs/telegraph`**: Required. Provides the `Adapter` interface and related types.
- **`decodelabs/coercion`**: Required. Used for type coercion in data handling.
- **`decodelabs/exceptional`**: Required. Used for exception handling throughout the adapter.
- **`decodelabs/monarch`**: Required. Used for logging exceptions.

### External

- **PHP**: See `composer.json` for supported PHP versions.
- **`mailchimp/marketing`**: Required. Mailchimp Marketing API v3 client library (^3.0.80).

### Optional

- **`decodelabs/nuance`**: Detected at runtime if installed, used for sensitive property marking (`SensitiveProperty` attribute).

## Behaviour & Contracts

### Invariants

- API key must be provided in settings.
- API key format: `{key}-{server}` (server extracted from API key).
- Email addresses are hashed using MD5 (lowercase, trimmed) for Mailchimp lookups.
- Consent field support is optional and must be explicitly enabled.
- Tag search count is limited to 1000 via `ListsApiOverride`.
- Group search count is limited to 1000.
- Interest category search count is limited to 1000.

### Input & Output Contracts

**Constructor:**
- Requires `apiKey` in settings array.
- Optional `consent` boolean in settings (default: `false`).
- Throws `InvalidArgument` exception if API key not provided.

**Fetch All List References:**
- Returns array of `ListReference` objects.
- Each reference contains: ID, name, creation date, subscribe URL, member count.
- Uses Mailchimp `getAllLists()` API.

**Fetch List Info:**
- Returns `ListInfo` object or `null` if list not found (404).
- Contains: ID, name, creation date, subscribe URL, member count.
- Fetches groups (interests) from interest categories.
- Fetches tags via `tagSearch()` API.
- Optionally fetches consent fields (marketing permissions) if enabled.
- Groups limited to 1000 per category.
- Tags limited to 1000.

**Subscribe:**
- Creates or updates member with `status = 'subscribed'`.
- Uses `setListMember()` API with `status_if_new = 'subscribed'`.
- Updates tags separately via `updateListMemberTags()` API.
- Returns `AdapterActionResult` with response and member info.
- Handles email, name, country, language, email type, groups, tags, and consent.

**Update:**
- Updates existing member data.
- Uses `setListMember()` API.
- Updates tags separately via `updateListMemberTags()` API.
- Returns `AdapterActionResult` with response and member info.
- Handles email, name, country, language, email type, groups, tags, and consent.

**Unsubscribe:**
- Updates member status to `'unsubscribed'`.
- Uses `updateListMember()` API.
- Returns `AdapterActionResult` with response (no member info).

**Fetch Member Info:**
- Returns `MemberInfo` object or `null` if member not found (404).
- Uses `getListMember()` API with specific fields.
- Normalizes Mailchimp status to `MemberStatus` enum.
- Extracts merge fields (FNAME, LNAME).
- Maps interests to groups.
- Maps tags to `TagInfo` objects.
- Maps marketing permissions to consent fields (if enabled).

**Error Handling:**
- HTTP 404 errors return `null` if `nullOn404 = true`.
- HTTP 400 errors with data request return `SubscriptionResponse` with failure reason:
  - `EmailInvalid`: Email address is fake or invalid
  - `Throttled`: Not allowing more signups
  - `Compliance`: Member is in compliance state (includes manual input URL)
- Other errors throw `Exceptional` exceptions (NotFound, BadRequest, Runtime).
- Tag update failures are logged but non-critical.

**Status Normalization:**
- `'subscribed'` → `MemberStatus::Subscribed`
- `'unsubscribed'` → `MemberStatus::Unsubscribed`
- `'cleaned'` → `MemberStatus::Invalid`
- `'pending'` → `MemberStatus::Pending`
- `'transactional'` → `MemberStatus::Subscribed`
- `'archived'` → `MemberStatus::Archived`
- Default → `MemberStatus::Archived`

**Email Type Mapping:**
- `EmailType::Html` → `'html'`
- `EmailType::Text` → `'text'`

**Groups (Interests):**
- Groups are Mailchimp interests within interest categories.
- Group ID is the interest ID.
- Category ID and name stored in `GroupInfo`.
- Groups passed as boolean map: `true` = enabled, `false` = disabled.

**Tags:**
- Tags can be passed as numeric IDs or tag names.
- Numeric IDs are resolved to tag names from `ListInfo`.
- Tags updated separately via `updateListMemberTags()` API.
- Tag status: `'active'` = enabled, `'inactive'` = disabled.

**Consent Fields (Marketing Permissions):**
- Only fetched if `consent` setting is enabled.
- Fetched from first member's marketing permissions (sample).
- Consent fields mapped to `ConsentField` objects.
- Consent passed as boolean map: `true` = enabled, `false` = disabled.
- Consent type inferred from description if not specified.

**Merge Fields:**
- `FNAME` → `firstName`
- `LNAME` → `lastName`
- Full name constructed from FNAME + LNAME.

**Location:**
- Country code extracted from `location.country_code`.

**Email Hashing:**
- Email addresses hashed using MD5 (lowercase, trimmed) for Mailchimp member lookups.
- Format: `md5(strtolower(trim($email)))`.

## Error Handling

- **Invalid API key**: Constructor throws `InvalidArgument` exception if API key not provided.
- **List not found**: `fetchListInfo()` returns `null` if list not found (404).
- **Member not found**: `fetchMemberInfo()` returns `null` if member not found (404).
- **Invalid email**: Returns `SubscriptionResponse` with `FailureReason::EmailInvalid` if email is fake or invalid.
- **Throttled**: Returns `SubscriptionResponse` with `FailureReason::Throttled` if not allowing more signups.
- **Compliance state**: Returns `SubscriptionResponse` with `FailureReason::Compliance` and `manualInputUrl` if member is in compliance state.
- **API errors**: HTTP errors throw `Exceptional` exceptions (NotFound, BadRequest, Runtime) with error messages from Mailchimp API.
- **Tag update failures**: Tag update failures are logged but non-critical (operation continues).

## Configuration & Extensibility

### Configuration

Adapter configured via settings array:

```php
$settings = [
    'apiKey' => 'your-api-key-here-us1', // Required
    'consent' => true, // Optional, default: false
];

$adapter = new Mailchimp($settings);
```

**Settings:**
- `apiKey` (string, required): Mailchimp API key (format: `{key}-{server}`)
- `consent` (bool, optional): Enable consent field support (default: `false`)

### Registration

Adapter registered via Archetype for `Adapter` interface:

```php
use DecodeLabs\Archetype;
use DecodeLabs\Telegraph\Adapter;
use DecodeLabs\Telegraph\Adapter\Mailchimp;

$archetype->registerType(Adapter::class, 'Mailchimp', Mailchimp::class);
```

### ListsApiOverride

The `ListsApiOverride` class extends Mailchimp's `ListsApi` to fix the tag search count limitation. It overrides `tagSearchRequest()` to set the count parameter to 1000, allowing retrieval of more than the default 10 tags.

## Interactions with Other Packages

- **Telegraph**: Implements `Adapter` interface from Telegraph. Provides Mailchimp-specific implementation for Telegraph operations.
- **Mailchimp Marketing API**: Uses `mailchimp/marketing` package for API client and ListsApi.
- **Coercion**: Used for type coercion in data handling.
- **Exceptional**: Used for exception handling throughout the adapter.
- **Monarch**: Used for logging exceptions (non-critical errors).
- **Nuance**: Detected at runtime if installed, used for sensitive property marking (`SensitiveProperty` attribute on API key).

## Usage Examples

### Basic Configuration

```php
use DecodeLabs\Telegraph\Adapter\Mailchimp;

$adapter = new Mailchimp([
    'apiKey' => 'your-api-key-here-us1',
    'consent' => true, // Enable consent field support
]);
```

### Fetch All Lists

```php
$lists = $adapter->fetchAllListReferences();

foreach ($lists as $list) {
    echo $list->name . ' (' . $list->id . ')' . PHP_EOL;
}
```

### Fetch List Info

```php
use DecodeLabs\Telegraph\SourceReference;

$source = new SourceReference('main', 'list-id-here');
$listInfo = $adapter->fetchListInfo($source);

echo $listInfo->name . PHP_EOL;
echo 'Groups: ' . count($listInfo->groups) . PHP_EOL;
echo 'Tags: ' . count($listInfo->tags) . PHP_EOL;
echo 'Consent Fields: ' . count($listInfo->consentFields) . PHP_EOL;
```

### Subscribe Member

```php
use DecodeLabs\Telegraph\MemberDataRequest;
use DecodeLabs\Telegraph\Source\ListInfo;

$request = new MemberDataRequest(
    email: 'test@example.com',
    firstName: 'Test',
    lastName: 'User',
    country: 'GB',
    language: 'en',
    groups: [
        'interest-id-1' => true,
        'interest-id-2' => false,
    ],
    tags: [
        'tag-name-1' => true,
        'tag-name-2' => false,
    ],
    consent: [
        'permission-id-1' => true,
    ]
);

$result = $adapter->subscribe($source, $listInfo, $request);

if ($result->response->success) {
    echo 'Subscription successful' . PHP_EOL;
    echo 'Status: ' . $result->response->status->value . PHP_EOL;
    echo 'Member ID: ' . $result->memberInfo->id . PHP_EOL;
}
```

### Update Member

```php
use DecodeLabs\Telegraph\MemberDataRequest;

$request = new MemberDataRequest(
    firstName: 'Updated',
    lastName: 'Name',
    groups: [
        'interest-id-1' => false,
        'interest-id-2' => true,
    ]
);

$result = $adapter->update($source, $listInfo, 'test@example.com', $request);

if ($result->response->success) {
    echo 'Update successful' . PHP_EOL;
}
```

### Unsubscribe Member

```php
$result = $adapter->unsubscribe($source, $listInfo, 'test@example.com');

if ($result->response->success) {
    echo 'Unsubscribe successful' . PHP_EOL;
    echo 'Status: ' . $result->response->status->value . PHP_EOL;
}
```

### Fetch Member Info

```php
$memberInfo = $adapter->fetchMemberInfo($source, $listInfo, 'test@example.com');

if ($memberInfo) {
    echo 'Email: ' . $memberInfo->email . PHP_EOL;
    echo 'Status: ' . $memberInfo->status->value . PHP_EOL;
    echo 'First Name: ' . $memberInfo->firstName . PHP_EOL;
    echo 'Last Name: ' . $memberInfo->lastName . PHP_EOL;
    echo 'Country: ' . $memberInfo->country . PHP_EOL;
    echo 'Language: ' . $memberInfo->language . PHP_EOL;
    
    foreach ($memberInfo->groups as $group) {
        echo 'Group: ' . $group->name . PHP_EOL;
    }
    
    foreach ($memberInfo->tags as $tag) {
        echo 'Tag: ' . $tag->name . PHP_EOL;
    }
}
```

### Error Handling

```php
try {
    $result = $adapter->subscribe($source, $listInfo, $request);
    
    if (!$result->response->success) {
        switch ($result->response->failureReason) {
            case FailureReason::EmailInvalid:
                echo 'Invalid email address' . PHP_EOL;
                break;
            case FailureReason::Throttled:
                echo 'Service is throttled' . PHP_EOL;
                break;
            case FailureReason::Compliance:
                echo 'Compliance issue - manual input required' . PHP_EOL;
                if ($result->response->manualInputUrl) {
                    echo 'URL: ' . $result->response->manualInputUrl . PHP_EOL;
                }
                break;
            case FailureReason::ServiceUnavailable:
                echo 'Service unavailable' . PHP_EOL;
                break;
        }
    }
} catch (Exceptional\NotFound $e) {
    echo 'List not found' . PHP_EOL;
} catch (Exceptional\BadRequest $e) {
    echo 'Bad request: ' . $e->getMessage() . PHP_EOL;
} catch (Exceptional\Runtime $e) {
    echo 'Runtime error: ' . $e->getMessage() . PHP_EOL;
}
```

## Implementation Notes (for Contributors)

### API Client Initialization

- API client initialized lazily on first use.
- Server extracted from API key (format: `{key}-{server}`).
- API key stored with `SensitiveProperty` attribute for security.

### ListsApiOverride

- Overrides `tagSearchRequest()` to set count parameter to 1000.
- Fixes limitation where Mailchimp API only returns 10 tags by default.
- Uses Guzzle HTTP PSR-7 `Uri` to modify query parameters.

### Error Handling

- `withListsApi()` wraps all API calls with error handling.
- Suppresses deprecated warnings during API calls.
- HTTP exceptions caught and converted to `Exceptional` exceptions.
- 404 errors return `null` if `nullOn404 = true`.
- 400 errors with data request return `SubscriptionResponse` with failure reason.
- Error messages extracted from Mailchimp API response body.

### Status Normalization

- Mailchimp status values normalized to `MemberStatus` enum.
- `'transactional'` status treated as `Subscribed`.
- Unknown statuses default to `Archived`.

### Email Hashing

- Email addresses hashed using MD5 (lowercase, trimmed) for Mailchimp lookups.
- Mailchimp requires MD5 hash for member operations.
- Format: `md5(strtolower(trim($email)))`.

### Groups (Interests)

- Groups are Mailchimp interests within interest categories.
- Interest categories fetched first, then interests for each category.
- Groups limited to 1000 per category.
- Category ID and name stored in `GroupInfo`.

### Tags

- Tags fetched via `tagSearch()` API (limited to 1000 via override).
- Tags can be passed as numeric IDs or tag names.
- Numeric IDs resolved to tag names from `ListInfo`.
- Tags updated separately via `updateListMemberTags()` API.
- Tag update failures are logged but non-critical.

### Consent Fields (Marketing Permissions)

- Consent fields only fetched if `consent` setting is enabled.
- Fetched from first member's marketing permissions (sample).
- Consent fields mapped to `ConsentField` objects.
- Consent type inferred from description if not specified.
- Consent passed as boolean map in `MemberDataRequest`.

### Merge Fields

- Standard merge fields: `FNAME` (first name), `LNAME` (last name).
- Full name constructed from FNAME + LNAME.
- Country code extracted from `location.country_code`.

### Date Handling

- Dates parsed using CarbonImmutable.
- Creation date: `timestamp_signup` or `timestamp_opt` fallback.
- Fetch date: current timestamp.

### Field Selection

- API calls use field selection to minimize data transfer.
- List info: `id`, `name`, `date_created`, `stats.member_count`, `subscribe_url_short`.
- Member info: `id`, `email_address`, `status`, `timestamp_signup`, `timestamp_opt`, `merge_fields.FNAME`, `merge_fields.LNAME`, `location.country_code`, `language`, `email_type`, `interests`, `tags`, `marketing_permissions`.

## Testing & Quality

**Current Status:**
- Code quality: Not yet assessed
- README quality: Basic (minimal documentation)
- Documentation: Not yet assessed
- Tests: Not yet assessed

**Testing Considerations:**
- Constructor should be tested for:
  - Valid API key
  - Missing API key (should throw exception)
  - Consent setting (enabled/disabled)

- List operations should be tested for:
  - Fetching all lists
  - Fetching list info (with groups, tags, consent fields)
  - List not found (404)

- Member operations should be tested for:
  - Subscribe (new member)
  - Subscribe (existing member - should update)
  - Update member data
  - Unsubscribe member
  - Member not found (404)

- Groups should be tested for:
  - Group management (add/remove)
  - Category handling
  - Group limit (1000)

- Tags should be tested for:
  - Tag management (add/remove)
  - Tag ID resolution
  - Tag name handling
  - Tag limit (1000)

- Consent fields should be tested for:
  - Consent field fetching (when enabled)
  - Consent management (add/remove)
  - Consent type inference

- Error handling should be tested for:
  - Invalid email (400)
  - Throttled (400)
  - Compliance state (400 with manual input URL)
  - List not found (404)
  - Member not found (404)
  - API errors (500, etc.)

- Edge cases should be tested for:
  - Empty groups/tags/consent
  - Missing merge fields
  - Missing location data
  - Tag update failures (non-critical)

## Roadmap & Future Ideas

- **Batch operations**: Support for batch subscribe/update/unsubscribe operations
- **Webhook handling**: Support for Mailchimp webhook processing
- **Campaign management**: Support for campaign creation and management
- **Analytics**: Support for Mailchimp analytics and reporting
- **Segment management**: Support for Mailchimp segment management
- **Automation workflows**: Support for Mailchimp automation workflows
- **Better error messages**: More detailed error messages from Mailchimp API
- **Performance optimization**: Caching and optimization for large-scale operations
- **Rate limiting**: Better handling of Mailchimp rate limits

## References

- Package repository: https://github.com/decodelabs/telegraph-mailchimp
- Composer package: https://packagist.org/packages/decodelabs/telegraph-mailchimp
- Related packages:
  - `decodelabs/telegraph` — Main Telegraph package
  - `mailchimp/marketing` — Mailchimp Marketing API client
- Mailchimp API documentation: https://mailchimp.com/developer/marketing/api/

