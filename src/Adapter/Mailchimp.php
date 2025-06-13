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
use DecodeLabs\Nuance\SensitiveProperty;
use DecodeLabs\Relay\Mailbox;
use DecodeLabs\Telegraph\Adapter;
use DecodeLabs\Telegraph\Source\EmailType;
use DecodeLabs\Telegraph\Source\GroupInfo;
use DecodeLabs\Telegraph\Source\ListInfo;
use DecodeLabs\Telegraph\Source\MemberInfo;
use DecodeLabs\Telegraph\Source\MemberStatus;
use DecodeLabs\Telegraph\Source\TagInfo;
use DecodeLabs\Telegraph\SubscriptionRequest;
use DecodeLabs\Telegraph\SubscriptionResponse;
use Exception;
use GuzzleHttp\Exception\ClientException as HttpClientException;
use MailchimpMarketing\ApiClient;
use Throwable;

class Mailchimp implements Adapter
{
    #[SensitiveProperty]
    protected string $apiKey;

    private ?ApiClient $apiClient = null;

    public function __construct(
        array $settings
    ) {
        // API Key
        if(null === ($apiKey = Coercion::tryString($settings['apiKey'] ?? null))) {
            throw Exceptional::InvalidArgument(
                'Mailchimp API key is required'
            );
        }

        $this->apiKey = $apiKey;
    }

    public function fetchListInfo(
        string $listId
    ): ?ListInfo {
        return $this->withApiClient(function(
            ApiClient $client
        ) use ($listId): ListInfo {
            // @phpstan-ignore-next-line
            $listResult = $client->lists->getList($listId, [
                'id', 'name', 'date_created', 'stats.member_count', 'subscribe_url_short'
            ]);


            // @phpstan-ignore-next-line
            $categoryResult = $client->lists->getListInterestCategories($listId, count: 100);
            $groups = [];

            foreach($categoryResult->categories as $category) {
                // @phpstan-ignore-next-line
                $groupResult = $client->lists->listInterestCategoryInterests($listId, $category->id, [
                    'interests.id', 'interests.name'
                ]);

                foreach($groupResult->interests as $group) {
                    $groups[] = new GroupInfo(
                        id: $group->id,
                        name: $group->name,
                        categoryId: $category->id,
                        categoryName: $category->title,
                    );
                }
            }


            // @phpstan-ignore-next-line
            $tagResult = $client->lists->tagSearch($listId);

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
                    fn($tag) => new TagInfo((string)$tag->id, $tag->name),
                    (array)($tagResult->tags ?? [])
                ),
            );
        }, nullOn404: true);
    }

    public function subscribe(
        SubscriptionRequest $request
    ): SubscriptionResponse {
        $request->subscribe = true;
        return $this->update($request);
    }

    public function update(
        SubscriptionRequest $request,
    ): SubscriptionResponse {
        return $this->withApiClient(function(
            ApiClient $client
        ) use ($request): SubscriptionResponse {
            $data = $mergeFields = [];

            if($request->subscribe) {
                $data['status'] = 'subscribed';
            }

            if($request->emailType !== null) {
                $data['email_type'] = match($request->emailType) {
                    EmailType::Html => 'html',
                    EmailType::Text => 'text'
                };
            }

            if($request->firstName !== null) {
                $mergeFields['FNAME'] = $request->firstName;
            }

            if($request->lastName !== null) {
                $mergeFields['LNAME'] = $request->lastName;
            }

            if($request->language !== null) {
                $data['language'] = $request->language;
            }

            if(!empty($mergeFields)) {
                $data['merge_fields'] = $mergeFields;
            }

            if(!empty($request->groups)) {
                $data['interests'] = $request->groups;
            }


            if(!empty($data)) {
                $data['email_address'] = $request->email;
                $data['status_if_new'] = 'subscribed';

                // @phpstan-ignore-next-line
                $result = $client->lists->setListMember(
                    $request->listId,
                    $this->hashEmail($request->email),
                    $data
                );

                $output = new SubscriptionResponse(
                    success: true,
                    subscribed: $result->status === 'subscribed',
                    bounced: $result->status === 'cleaned',
                    invalid: $result->status === 'cleaned',
                    mailbox: new Mailbox($result->email_address, $this->getMergeName($result->merge_fields)),
                );
            } else {
                // @phpstan-ignore-next-line
                $result = $client->lists->getListMember($request->listId, $this->hashEmail($request->email), [
                    'status',
                    'merge_fields.FNAME', 'merge_fields.LNAME',
                ]);

                $output = new SubscriptionResponse(
                    success: true,
                    subscribed: $result->status === 'subscribed',
                    bounced: $result->status === 'cleaned',
                    invalid: $result->status === 'cleaned',
                    mailbox: new Mailbox($request->email, $this->getMergeName($result->merge_fields))
                );
            }


            // Call separately
            if(!empty($request->tags)) {
                try {
                    // @phpstan-ignore-next-line
                    $client->lists->updateListMemberTags(
                        $request->listId,
                        $this->hashEmail($request->email),
                        [
                            'tags' => array_map(
                                fn($tag, $enabled) => [
                                    'name' => $tag,
                                    'status' => $enabled ?
                                        'active' :
                                        'inactive'
                                ],
                                array_keys($request->tags),
                                array_values($request->tags)
                            )
                        ]
                    );
                } catch (Throwable $e) {
                    // This is a non-critical operation
                }
            }

            return $output;
        }, subscriptionRequest: $request);
    }

    public function unsubscribe(
        string $listId,
        string $email
    ): SubscriptionResponse {
        return $this->withApiClient(function(
            ApiClient $client
        ) use ($listId, $email): SubscriptionResponse {
            // @phpstan-ignore-next-line
            $result = $client->lists->updateListMember(
                $listId,
                $this->hashEmail($email),
                ['status' => 'unsubscribed']
            );

            return new SubscriptionResponse(
                success: true,
                subscribed: false,
                mailbox: new Mailbox($result->email_address, $this->getMergeName($result->merge_fields)),
            );
        }, subscriptionRequest: new SubscriptionRequest(
            listId: $listId,
            email: $email
        ));
    }

    public function fetchMemberInfo(
        string $listId,
        string $email
    ): ?MemberInfo {
        return $this->withApiClient(function(
            ApiClient $client
        ) use ($listId, $email): MemberInfo {
            // @phpstan-ignore-next-line
            $result = $client->lists->getListMember($listId, $this->hashEmail($email), [
                'id', 'email_address', 'status',
                'timestamp_signup', 'timestamp_opt',
                'merge_fields.FNAME', 'merge_fields.LNAME',
                'location.country_code', 'language', 'email_type',
                'interests', 'tags'
            ]);

            return new MemberInfo(
                id: $result->id,
                email: $result->email_address,
                status: match($result->status) {
                    'subscribed' => MemberStatus::Subscribed,
                    'unsubscribed' => MemberStatus::Unsubscribed,
                    'cleaned' => MemberStatus::Invalid,
                    'pending' => MemberStatus::Pending,
                    'transactional' => MemberStatus::Subscribed,
                    'archived' => MemberStatus::Archived,
                    default => MemberStatus::Archived
                },
                creationDate: $result->timestamp_signup
                    ? CarbonImmutable::parse($result->timestamp_signup)
                    : (
                        $result->timestamp_opt ?
                            CarbonImmutable::parse($result->timestamp_opt) :
                            null
                    ),
                firstName: $result->merge_fields->FNAME ?: null,
                lastName: $result->merge_fields->LNAME ?: null,
                country: $result->location->country_code ?? null,
                language: $result->language ?? null,
                emailType: match($result->email_type) {
                    'html' => EmailType::Html,
                    'text' => EmailType::Text,
                    default => null
                },
                groupIds: array_filter(
                    array_map(
                        fn($enabled, $id): ?string => $enabled ? (string)$id : null,
                        $i = (array)($result->interests ?? []),
                        array_keys($i)
                    ),
                    fn(?string $id) => $id !== null
                ),
                tags: array_map(
                    fn($tag) => new TagInfo((string)$tag->id, $tag->name),
                    (array)($result->tags ?? [])
                )
            );
        }, nullOn404: true);
    }

    /**
     * Wrap calls to redirect Exceptions and suppress deprecated warnings
     *
     * @template TReturn
     * @param callable(ApiClient): TReturn $callback
     * @return ($nullOn404 is true ? ?TReturn : ($subscriptionRequest is null ? TReturn : SubscriptionResponse))
     */
    protected function withApiClient(
        callable $callback,
        bool $nullOn404 = false,
        ?SubscriptionRequest $subscriptionRequest = null
    ): mixed {
        try {
            $errorReporting = error_reporting();
            error_reporting($errorReporting & ~E_DEPRECATED);
            $client = $this->getApiClient();
            $output = $callback($client);
            error_reporting($errorReporting);
            return $output;
        } catch (HttpClientException $e) {
            $response = $e->getResponse();
            $status = $response->getStatusCode();

            if(
                $status === 404 &&
                $nullOn404
            ) {
                return null;
            }

            $data = Coercion::toArray(
                json_decode($response->getBody()->getContents(), true)
            );


            if(
                $status === 400 &&
                $subscriptionRequest
            ) {
                $result = new SubscriptionResponse(
                    success: false,
                    mailbox: new Mailbox(
                        $subscriptionRequest->email,
                        $subscriptionRequest->fullName
                    )
                );

                if (preg_match('/fake or invalid/i', $e->getMessage())) {
                    $result->invalid = true;
                } elseif (preg_match('/not allowing more signups for now/i', $e->getMessage())) {
                    $result->throttled = true;
                } elseif (preg_match('/is in a compliance state/i', $e->getMessage())) {
                    $result->requiresManualInput = true;

                    try {
                        // @phpstan-ignore-next-line
                        $listResult = $client->lists->getList($subscriptionRequest->listId, [
                            'subscribe_url_short'
                        ]);
                        $result->manualInputUrl = $listResult->subscribe_url_short ?? null;
                    } catch(Throwable $e) {
                    }
                }

                return $result;
            }


            $exceptionType = match($status) {
                404 => 'NotFound',
                400 => 'BadRequest',
                default => 'Runtime'
            };

            throw Exceptional::{$exceptionType}(
                message: Coercion::toString($data['detail'] ?? $e->getMessage()),
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

    protected function hashEmail(
        string $email
    ): string {
        return md5(strtolower(trim($email)));
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
