<?php

/**
 * @package Telegraph
 * @license http://opensource.org/licenses/MIT
 */

declare(strict_types=1);

namespace DecodeLabs\Telegraph\Adapter;

use Carbon\CarbonImmutable;
use DecodeLabs\Coercion;
use DecodeLabs\Exceptional;
use DecodeLabs\Exceptional\Exception as ExceptionalException;
use DecodeLabs\Monarch;
use DecodeLabs\Nuance\SensitiveProperty;
use DecodeLabs\Relay\Mailbox;
use DecodeLabs\Telegraph\Adapter;
use DecodeLabs\Telegraph\Adapter\Mailchimp\ListsApiOverride;
use DecodeLabs\Telegraph\AdapterActionResult;
use DecodeLabs\Telegraph\FailureReason;
use DecodeLabs\Telegraph\MemberDataRequest;
use DecodeLabs\Telegraph\Source\ConsentField;
use DecodeLabs\Telegraph\Source\EmailType;
use DecodeLabs\Telegraph\Source\GroupInfo;
use DecodeLabs\Telegraph\Source\ListInfo;
use DecodeLabs\Telegraph\Source\ListReference;
use DecodeLabs\Telegraph\Source\MemberInfo;
use DecodeLabs\Telegraph\Source\MemberStatus;
use DecodeLabs\Telegraph\Source\TagInfo;
use DecodeLabs\Telegraph\SourceReference;
use DecodeLabs\Telegraph\SubscriptionResponse;
use Exception;
use GuzzleHttp\Exception\ClientException as HttpClientException;
use MailchimpMarketing\Api\ListsApi;
use MailchimpMarketing\ApiClient;
use Throwable;

class Mailchimp implements Adapter
{
    #[SensitiveProperty]
    protected string $apiKey;
    protected bool $supportConsent = false;

    private ?ApiClient $apiClient = null;

    public function __construct(
        array $settings
    ) {
        // API Key
        if (null === ($apiKey = Coercion::tryString($settings['apiKey'] ?? null))) {
            throw Exceptional::InvalidArgument(
                'Mailchimp API key is required'
            );
        }

        $this->apiKey = $apiKey;

        // Support consent
        $this->supportConsent = Coercion::toBool($settings['consent'] ?? false);
    }

    public function fetchAllListReferences(): array
    {
        return $this->withListsApi(
            action: function (
                ListsApi $api
            ): array {
                $listResult = $api->getAllLists();
                $output = [];

                foreach ($listResult->lists as $list) {
                    $output[] = new ListReference(
                        id: $list->id,
                        name: $list->name,
                        creationDate: $list->date_created
                            ? CarbonImmutable::parse($list->date_created)
                            : null,
                        subscribeUrl: $list->subscribe_url_short ?? null,
                        memberCount: $list->stats->member_count ?? null,
                    );
                }

                return $output;
            }
        );
    }

    public function fetchListInfo(
        SourceReference $source
    ): ?ListInfo {
        return $this->withListsApi(
            source: $source,
            nullOn404: true,
            action: function (
                ListsApi $api
            ) use ($source): ListInfo {
                // List info
                $listResult = $api->getList($source->remoteId, [
                    'id', 'name', 'date_created', 'stats.member_count', 'subscribe_url_short'
                ]);

                // Groups
                $categoryResult = $api->getListInterestCategories($source->remoteId, count: 1000);
                $groups = [];

                foreach ($categoryResult->categories as $category) {
                    $groupResult = $api->listInterestCategoryInterests($source->remoteId, $category->id, [
                        'interests.id', 'interests.name'
                    ], count: 1000);

                    foreach ($groupResult->interests as $group) {
                        $groups[] = new GroupInfo(
                            id: $group->id,
                            name: $group->name,
                            categoryId: $category->id,
                            categoryName: $category->title,
                        );
                    }
                }


                // Tags
                $tagResult = $api->tagSearch($source->remoteId);


                // Consent fields
                $permissions = [];

                if ($this->supportConsent) {
                    try {
                        $consentResult = $api->getListMembersInfo($source->remoteId, ['members.marketing_permissions'], count: 1);
                        $permissions = $consentResult->members[0]->marketing_permissions ?? [];
                    } catch (Throwable $e) {
                        Monarch::logException($e);
                    }
                }

                return new ListInfo(
                    id: $listResult->id,
                    name: $listResult->name,
                    creationDate: $listResult->date_created
                        ? CarbonImmutable::parse($listResult->date_created)
                        : null,
                    subscribeUrl: $listResult->subscribe_url_short ?? null,
                    memberCount: $listResult->stats->member_count ?? null,
                    groups: $groups,
                    tags: array_map(
                        fn ($tag) => new TagInfo((string)$tag->id, $tag->name),
                        (array)($tagResult->tags ?? [])
                    ),
                    consentFields: array_map(
                        fn ($permission) => new ConsentField(
                            id: $permission->marketing_permission_id,
                            description: $permission->text,
                        ),
                        $permissions
                    ),
                );
            }
        );
    }

    public function subscribe(
        SourceReference $source,
        ListInfo $listInfo,
        MemberDataRequest $request
    ): AdapterActionResult {
        if ($request->email === null) {
            throw Exceptional::InvalidArgument(
                'Email address is required'
            );
        }

        return $this->updateMember($source, $listInfo, $request->email, $request, true);
    }

    public function update(
        SourceReference $source,
        ListInfo $listInfo,
        string $email,
        MemberDataRequest $request,
    ): AdapterActionResult {
        return $this->updateMember($source, $listInfo, $email, $request, false);
    }

    protected function updateMember(
        SourceReference $source,
        ListInfo $listInfo,
        string $email,
        MemberDataRequest $request,
        bool $subscribe = false
    ): AdapterActionResult {
        return $this->withListsApi(
            source: $source,
            dataRequest: $request,
            action: function (
                ListsApi $api
            ) use ($source, $listInfo, $email, $request, $subscribe): AdapterActionResult {
                $data = $mergeFields = [];

                if ($request->email !== null) {
                    $data['email_address'] = $request->email;
                }

                if ($subscribe) {
                    $data['status'] = 'subscribed';
                }

                if ($request->emailType !== null) {
                    $data['email_type'] = match ($request->emailType) {
                        EmailType::Html => 'html',
                        EmailType::Text => 'text'
                    };
                }

                if ($request->firstName !== null) {
                    $mergeFields['FNAME'] = $request->firstName;
                }

                if ($request->lastName !== null) {
                    $mergeFields['LNAME'] = $request->lastName;
                }

                if ($request->language !== null) {
                    $data['language'] = $request->language;
                }

                if (!empty($mergeFields)) {
                    $data['merge_fields'] = $mergeFields;
                }

                if (!empty($request->groups)) {
                    $data['interests'] = $request->groups;
                }

                if (
                    $this->supportConsent &&
                    !empty($request->consent)
                ) {
                    $data['marketing_permissions'] = array_map(
                        fn ($intent, $id) => [
                            'marketing_permission_id' => $id,
                            'enabled' => $intent
                        ],
                        $request->consent,
                        array_keys($request->consent)
                    );
                }

                $result = null;


                // Update member info
                if (!empty($data)) {
                    $data['email_address'] = $request->email;
                    $data['status_if_new'] = 'subscribed';

                    $result = $api->setListMember(
                        $source->remoteId,
                        $this->hashEmail($email),
                        $data
                    );
                }


                // Update tags separately
                $tagsUpdated = false;

                if (!empty($request->tags)) {
                    $tags = [];

                    foreach ($request->tags as $id => $intent) {
                        if (
                            is_numeric($id) &&
                            isset($listInfo->tags[$id])
                        ) {
                            $id = $listInfo->tags[$id]->name;
                        }

                        $tags[] = [
                            'name' => $id,
                            'status' => $intent ?
                                'active' :
                                'inactive'
                        ];
                    }


                    try {
                        $api->updateListMemberTags(
                            $source->remoteId,
                            $this->hashEmail($email),
                            ['tags' => $tags]
                        );

                        $tagsUpdated = true;
                    } catch (Throwable $e) {
                        // This is a non-critical operation
                        Monarch::logException($e);
                        $tagsUpdated = false;
                    }
                }


                // Fetch member info if no user data was provided
                if (empty($data)) {
                    $result = $api->getListMember($source->remoteId, $this->hashEmail($email));
                }



                // Create tag map for output
                $tagMap = [];

                foreach ((array)($result->tags ?? []) as $tag) {
                    $tagMap[$tag->id] = new TagInfo((string)$tag->id, $tag->name);
                }


                // Update tag map if tags were updated
                if (
                    !empty($data) &&
                    $tagsUpdated
                ) {
                    foreach ($request->tags as $id => $intent) {
                        if (!$intent) {
                            unset($tagMap[$id]);
                            continue;
                        }

                        if (
                            !isset($tagMap[$id]) ||
                            !isset($listInfo->tags[$id])
                        ) {
                            continue;
                        }

                        $tagMap[$id] = $listInfo->tags[$id];
                    }
                }


                // Return result
                return new AdapterActionResult(
                    new SubscriptionResponse(
                        source: $source,
                        success: true,
                        status: $this->normalizeStatus($result->status),
                        mailbox: new Mailbox($result->email_address, $this->getMergeName($result->merge_fields))
                    ),
                    new MemberInfo(
                        id: $result->id,
                        email: $result->email_address,
                        status: $this->normalizeStatus($result->status),
                        creationDate: $result->timestamp_signup
                            ? CarbonImmutable::parse($result->timestamp_signup)
                            : (
                                $result->timestamp_opt ?
                                    CarbonImmutable::parse($result->timestamp_opt) :
                                    null
                            ),
                        firstName: $result->merge_fields->FNAME ?: null,
                        lastName: $result->merge_fields->LNAME ?: null,
                        country: $result->location->country_code ?: null,
                        language: $result->language ?? null,
                        emailType: match ($result->email_type) {
                            'html' => EmailType::Html,
                            'text' => EmailType::Text,
                            default => null
                        },
                        groups: array_filter(
                            array_map(
                                fn ($enabled, $id): ?GroupInfo => (
                                    $enabled && ($group = ($listInfo->groups[$id] ?? null))
                                ) ?
                                    $group :
                                    null,
                                $i = (array)($result->interests ?? []),
                                array_keys($i)
                            ),
                            fn (?GroupInfo $group) => $group !== null
                        ),
                        tags: $tagMap,
                        consent: array_filter(
                            array_map(
                                fn ($field): ?ConsentField => $field->enabled ?
                                    new ConsentField(
                                        id: Coercion::asString($field->marketing_permission_id),
                                        description: Coercion::asString($field->text),
                                    ) :
                                    null,
                                (array)($result->marketing_permissions ?? [])
                            ),
                            fn (?ConsentField $consent) => $consent !== null
                        )
                    )
                );
            }
        );
    }

    public function unsubscribe(
        SourceReference $source,
        ListInfo $listInfo,
        string $email
    ): AdapterActionResult {
        return $this->withListsApi(
            source: $source,
            dataRequest: new MemberDataRequest(
                email: $email
            ),
            action: function (
                ListsApi $api
            ) use ($source, $email): AdapterActionResult {
                $result = $api->updateListMember(
                    $source->remoteId,
                    $this->hashEmail($email),
                    ['status' => 'unsubscribed']
                );

                return new AdapterActionResult(
                    new SubscriptionResponse(
                        source: $source,
                        success: true,
                        status: $this->normalizeStatus($result->status),
                        mailbox: new Mailbox($result->email_address, $this->getMergeName($result->merge_fields)),
                    )
                );
            }
        );
    }

    public function fetchMemberInfo(
        SourceReference $source,
        ListInfo $listInfo,
        string $email
    ): ?MemberInfo {
        return $this->withListsApi(
            source: $source,
            nullOn404: true,
            action: function (
                ListsApi $api
            ) use ($source, $listInfo, $email): MemberInfo {
                $result = $api->getListMember($source->remoteId, $this->hashEmail($email), [
                    'id', 'email_address', 'status',
                    'timestamp_signup', 'timestamp_opt',
                    'merge_fields.FNAME', 'merge_fields.LNAME',
                    'location.country_code', 'language', 'email_type',
                    'interests', 'tags', 'marketing_permissions'
                ]);

                return new MemberInfo(
                    id: $result->id,
                    email: $result->email_address,
                    status: $this->normalizeStatus($result->status),
                    creationDate: $result->timestamp_signup
                        ? CarbonImmutable::parse($result->timestamp_signup)
                        : (
                            $result->timestamp_opt ?
                                CarbonImmutable::parse($result->timestamp_opt) :
                                null
                        ),
                    firstName: $result->merge_fields->FNAME ?: null,
                    lastName: $result->merge_fields->LNAME ?: null,
                    country: $result->location->country_code ?: null,
                    language: $result->language ?? null,
                    emailType: match ($result->email_type) {
                        'html' => EmailType::Html,
                        'text' => EmailType::Text,
                        default => null
                    },
                    groups: array_filter(
                        array_map(
                            fn ($enabled, $id): ?GroupInfo => (
                                $enabled && ($group = ($listInfo->groups[$id] ?? null))
                            ) ?
                                $group :
                                null,
                            $i = (array)($result->interests ?? []),
                            array_keys($i)
                        ),
                        fn (?GroupInfo $group) => $group !== null
                    ),
                    tags: array_map(
                        fn ($tag) => new TagInfo((string)$tag->id, $tag->name),
                        (array)($result->tags ?? [])
                    ),
                    consent: array_filter(
                        array_map(
                            fn ($field): ?ConsentField => $field->enabled ?
                                new ConsentField(
                                    id: Coercion::asString($field->marketing_permission_id),
                                    description: Coercion::asString($field->text),
                                ) :
                                null,
                            (array)($result->marketing_permissions ?? [])
                        ),
                        fn (?ConsentField $consent) => $consent !== null
                    )
                );
            }
        );
    }

    /**
     * Wrap calls to redirect Exceptions and suppress deprecated warnings
     *
     * @template TReturn
     * @param callable(ListsApi): TReturn $action
     * @return ($nullOn404 is true ? ?TReturn : ($dataRequest is null ? TReturn : AdapterActionResult))
     */
    protected function withListsApi(
        callable $action,
        ?SourceReference $source = null,
        ?MemberDataRequest $dataRequest = null,
        bool $nullOn404 = false,
    ): mixed {
        $errorReporting = error_reporting();
        error_reporting($errorReporting & ~E_DEPRECATED);
        $api = $this->getListsApi();
        error_reporting($errorReporting);

        try {
            $output = $action($api);
            return $output;
        } catch (HttpClientException $e) {
            $response = $e->getResponse();
            $status = $response->getStatusCode();

            if (
                $status === 404 &&
                $nullOn404
            ) {
                return null;
            }

            if (!$source) {
                throw Exceptional::Runtime(
                    message: 'No source provided',
                    previous: $e
                );
            }

            $data = Coercion::toArray(
                json_decode($response->getBody()->getContents(), true)
            );

            $message = Coercion::toString($data['detail'] ?? $e->getMessage());



            if (
                $status === 400 &&
                $dataRequest
            ) {
                $response = new SubscriptionResponse(
                    source: $source,
                    success: false,
                );

                if (preg_match('/fake or invalid/i', $message)) {
                    $response->failureReason = FailureReason::EmailInvalid;
                } elseif (preg_match('/not allowing more signups for now/i', $message)) {
                    $response->failureReason = FailureReason::Throttled;
                } elseif (preg_match('/is in a compliance state/i', $message)) {
                    $response->failureReason = FailureReason::Compliance;

                    try {
                        $listResult = $api->getList($source->remoteId, [
                            'subscribe_url_short'
                        ]);

                        $response->manualInputUrl = $listResult->subscribe_url_short ?? null;
                    } catch (Throwable $e) {
                    }
                }

                Monarch::logException(
                    Exceptional::Runtime(
                        message: $message,
                    )
                );

                return new AdapterActionResult($response);
            }


            $exceptionType = match ($status) {
                404 => 'NotFound',
                400 => 'BadRequest',
                default => 'Runtime'
            };

            throw Exceptional::{$exceptionType}(
                message: $message,
                http: Coercion::toInt($data['status'] ?? $status),
                //previous: $e
            );
        } catch (ExceptionalException $e) {
            throw $e;
        } catch (Exception $e) {
            throw Exceptional::Runtime(
                message: $e->getMessage(),
                previous: $e
            );
        }
    }

    protected function getApiClient(): ApiClient
    {
        if ($this->apiClient === null) {
            $this->apiClient = new ApiClient();

            $this->apiClient->setConfig([
                'apiKey' => $this->apiKey,
                'server' => substr($this->apiKey, strpos($this->apiKey, '-') + 1)
            ]);
        }

        return $this->apiClient;
    }

    protected function getListsApi(): ListsApi
    {
        $client = $this->getApiClient();
        return new ListsApiOverride($client);
    }

    protected function hashEmail(
        string $email
    ): string {
        return md5(strtolower(trim($email)));
    }

    protected function normalizeStatus(
        string $status
    ): MemberStatus {
        return match ($status) {
            'subscribed' => MemberStatus::Subscribed,
            'unsubscribed' => MemberStatus::Unsubscribed,
            'cleaned' => MemberStatus::Invalid,
            'pending' => MemberStatus::Pending,
            'transactional' => MemberStatus::Subscribed,
            'archived' => MemberStatus::Archived,
            default => MemberStatus::Archived
        };
    }

    protected function getMergeName(
        ?object $mergeFields
    ): ?string {
        if ($mergeFields === null) {
            return null;
        }

        $name = trim(
            ($mergeFields->FNAME ?? '') . ' ' .
            ($mergeFields->LNAME ?? '')
        );

        return empty($name) ? null : $name;
    }
}
