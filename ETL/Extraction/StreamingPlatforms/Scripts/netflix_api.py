import requests
import pandas as pd
import pprint
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


master_data = []

for year in range(2019, 2025):
    print(f"Year ---> {year}")
    for page in range(1, 100):
        print(f"Page ----> {page}")
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'app-version': '3.8.2-web-web',
            'content-type': 'application/json',
            'device-id': 'pwVqYQKnEe-XwVJuEa95RA',
            'jw-session': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWRpZW5jZSI6InNlc3Npb24iLCJleHAiOjMyMjk1NzQzNDUsImlhdCI6MTcxNTg0NjM0NSwiaXNzdWVyIjoianVzdHdhdGNoIiwic3ViIjoicHdWcVlRS25FZS1Yd1ZKdUVhOTVSQSJ9.KKs77FQyD-hgnMarVDpO4WJqH0iscjYNHQQps4vTVoo',
            'origin': 'https://www.justwatch.com',
            'priority': 'u=1, i',
            'referer': 'https://www.justwatch.com/',
            'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }

        json_data = {
            'operationName': 'GetPopularTitles',
            'variables': {
                'first': 100,
                'platform': 'WEB',
                'popularTitlesSortBy': 'POPULAR',
                'sortRandomSeed': 0,
                'offset': (page-1)*100,
                'creditsRole': 'DIRECTOR',
                'after': 'MTMwMA==',
                'popularTitlesFilter': {
                    'ageCertifications': [],
                    'excludeGenres': [],
                    'excludeProductionCountries': [],
                    'objectTypes': [],
                    'productionCountries': [],
                    'subgenres': [],
                    'genres': [],
                    'packages': [
                        'nfx',
                    ],
                    'excludeIrrelevantTitles': False,
                    'presentationTypes': [],
                    'monetizationTypes': [],
                    'releaseYear': {
                        'min': year,
                        'max': year
                    },
                    'searchQuery': '',
                },
                'watchNowFilter': {
                    'packages': [
                        'nfx',
                    ],
                    'monetizationTypes': [],
                },
                'language': 'en',
                'country': 'IN',
                'allowSponsoredRecommendations': {
                    'pageType': 'VIEW_POPULAR',
                    'placement': 'POPULAR_VIEW',
                    'country': 'IN',
                    'language': 'en',
                    'appId': '3.8.2-webapp#ad0d029',
                    'platform': 'WEB',
                    'supportedFormats': [
                        'IMAGE',
                        'VIDEO',
                    ],
                    'supportedObjectTypes': [
                        'MOVIE',
                        'SHOW',
                        'GENERIC_TITLE_LIST',
                        'SHOW_SEASON',
                    ],
                    'testingMode': True,
                    'testingModeCampaignName': None,
                },
            },
            'query': 'query GetPopularTitles($allowSponsoredRecommendations: SponsoredRecommendationsInput, $backdropProfile: BackdropProfile, $country: Country!, $first: Int! = 70, $format: ImageFormat, $language: Language!, $platform: Platform! = WEB, $after: String, $popularTitlesFilter: TitleFilter, $popularTitlesSortBy: PopularTitlesSorting! = POPULAR, $profile: PosterProfile, $sortRandomSeed: Int! = 0, $watchNowFilter: WatchNowOfferFilter!, $offset: Int = 0, $creditsRole: CreditRole! = DIRECTOR) {\n  popularTitles(\n    allowSponsoredRecommendations: $allowSponsoredRecommendations\n    country: $country\n    filter: $popularTitlesFilter\n    first: $first\n    sortBy: $popularTitlesSortBy\n    sortRandomSeed: $sortRandomSeed\n    offset: $offset\n    after: $after\n  ) {\n    __typename\n    edges {\n      cursor\n      node {\n        ...PopularTitleGraphql\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      startCursor\n      endCursor\n      hasPreviousPage\n      hasNextPage\n      __typename\n    }\n    sponsoredAd {\n      ...SponsoredAd\n      __typename\n    }\n    totalCount\n  }\n}\n\nfragment PopularTitleGraphql on MovieOrShow {\n  id\n  objectId\n  objectType\n  content(country: $country, language: $language) {\n    title\n    fullPath\n    scoring {\n      imdbVotes\n      imdbScore\n      tmdbPopularity\n      tmdbScore\n      __typename\n    }\n    dailymotionClips: clips(providers: [DAILYMOTION]) {\n      sourceUrl\n      externalId\n      provider\n      __typename\n    }\n    posterUrl(profile: $profile, format: $format)\n    ... on MovieOrShowOrSeasonContent {\n      backdrops(profile: $backdropProfile, format: $format) {\n        backdropUrl\n        __typename\n      }\n      __typename\n    }\n    isReleased\n    credits(role: $creditsRole) {\n      name\n      personId\n      __typename\n    }\n    scoring {\n      imdbVotes\n      __typename\n    }\n    runtime\n    genres {\n      translation(language: $language)\n      shortName\n      __typename\n    }\n    __typename\n  }\n  likelistEntry {\n    createdAt\n    __typename\n  }\n  dislikelistEntry {\n    createdAt\n    __typename\n  }\n  watchlistEntryV2 {\n    createdAt\n    __typename\n  }\n  customlistEntries {\n    createdAt\n    __typename\n  }\n  freeOffersCount: offerCount(\n    country: $country\n    platform: WEB\n    filter: {monetizationTypes: [FREE, ADS]}\n  )\n  watchNowOffer(country: $country, platform: WEB, filter: $watchNowFilter) {\n    id\n    standardWebURL\n    package {\n      id\n      packageId\n      clearName\n      __typename\n    }\n    retailPrice(language: $language)\n    retailPriceValue\n    lastChangeRetailPriceValue\n    currency\n    presentationType\n    monetizationType\n    availableTo\n    __typename\n  }\n  ... on Movie {\n    seenlistEntry {\n      createdAt\n      __typename\n    }\n    __typename\n  }\n  ... on Show {\n    tvShowTrackingEntry {\n      createdAt\n      __typename\n    }\n    seenState(country: $country) {\n      seenEpisodeCount\n      progress\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment SponsoredAd on SponsoredRecommendationAd {\n  bidId\n  holdoutGroup\n  campaign {\n    name\n    externalTrackers {\n      type\n      data\n      __typename\n    }\n    hideRatings\n    hideDetailPageButton\n    promotionalImageUrl\n    promotionalVideo {\n      url\n      __typename\n    }\n    promotionalTitle\n    promotionalText\n    promotionalProviderLogo\n    watchNowLabel\n    watchNowOffer {\n      standardWebURL\n      presentationType\n      monetizationType\n      package {\n        id\n        packageId\n        shortName\n        clearName\n        icon\n        __typename\n      }\n      __typename\n    }\n    nodeOverrides {\n      nodeId\n      promotionalImageUrl\n      watchNowOffer {\n        standardWebURL\n        __typename\n      }\n      __typename\n    }\n    node {\n      nodeId: id\n      __typename\n      ... on MovieOrShowOrSeason {\n        content(country: $country, language: $language) {\n          fullPath\n          posterUrl\n          title\n          originalReleaseYear\n          scoring {\n            imdbScore\n            __typename\n          }\n          externalIds {\n            imdbId\n            __typename\n          }\n          backdrops(format: $format, profile: $backdropProfile) {\n            backdropUrl\n            __typename\n          }\n          isReleased\n          __typename\n        }\n        objectId\n        objectType\n        offers(country: $country, platform: $platform) {\n          monetizationType\n          presentationType\n          package {\n            id\n            packageId\n            __typename\n          }\n          id\n          __typename\n        }\n        __typename\n      }\n      ... on MovieOrShow {\n        watchlistEntryV2 {\n          createdAt\n          __typename\n        }\n        __typename\n      }\n      ... on Show {\n        seenState(country: $country) {\n          seenEpisodeCount\n          __typename\n        }\n        __typename\n      }\n      ... on Season {\n        content(country: $country, language: $language) {\n          seasonNumber\n          __typename\n        }\n        show {\n          __typename\n          id\n          content(country: $country, language: $language) {\n            originalTitle\n            __typename\n          }\n          watchlistEntryV2 {\n            createdAt\n            __typename\n          }\n        }\n        __typename\n      }\n      ... on GenericTitleList {\n        followedlistEntry {\n          createdAt\n          name\n          __typename\n        }\n        id\n        type\n        content(country: $country, language: $language) {\n          name\n          visibility\n          __typename\n        }\n        titles(country: $country, first: 40) {\n          totalCount\n          edges {\n            cursor\n            node {\n              content(country: $country, language: $language) {\n                fullPath\n                posterUrl\n                title\n                originalReleaseYear\n                scoring {\n                  imdbScore\n                  __typename\n                }\n                isReleased\n                __typename\n              }\n              id\n              objectId\n              objectType\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n    }\n    __typename\n  }\n  __typename\n}\n',
        }

        response = requests.post('https://apis.justwatch.com/graphql', headers=headers, json=json_data)
        justwatch_movies = response.json()["data"]["popularTitles"]["edges"]
        print(f"No. of Movies Found ---> {len(justwatch_movies)}")
        if len(justwatch_movies) == 0:
            break
        for movies in justwatch_movies:
            data_dict = {}
            title = movies["node"]["content"]["title"]
            url_trail = movies["node"]["content"]["fullPath"]
            imdb_score = movies["node"]["content"]["scoring"]["imdbScore"]
            type = movies["node"]["objectType"]
            runtime = movies["node"]["content"]["runtime"]
            genres = ', '.join([elem["translation"] for elem in movies["node"]["content"]["genres"]])
            standard_url = movies["node"]["watchNowOffer"]["standardWebURL"]
            data_dict['Show/Movie Title'] = title
            data_dict['Type'] = type
            data_dict['Show/MovieLink'] = 'https://www.justwatch.com' + url_trail
            data_dict['IMDB'] = imdb_score
            data_dict['Runtime'] = f"{runtime} mins"
            data_dict['Genres'] = genres
            # data_dict['Standard URL'] = standard_url
            # print(title, url_trail, imdb_score, runtime, genres, standard_url)
            headers = {
                'authority': 'apis.justwatch.com',
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'app-version': '3.8.2-web-web',
                'content-type': 'application/json',
                'device-id': 'pwVqYQKnEe-XwVJuEa95RA',
                'jw-session': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWRpZW5jZSI6InNlc3Npb24iLCJleHAiOjMyMjc3NTE5NTIsImlhdCI6MTcxNDAyMzk1MiwiaXNzdWVyIjoianVzdHdhdGNoIiwic3ViIjoicHdWcVlRS25FZS1Yd1ZKdUVhOTVSQSJ9._QmNfyVH2RmBGrYv1Ln57AmItdufL1OVTPKTzUnqvxg',
                'origin': 'https://www.justwatch.com',
                'referer': 'https://www.justwatch.com/',
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            }

            json_data = {
                'operationName': 'GetUrlTitleDetails',
                'variables': {
                    'platform': 'WEB',
                    'fullPath': url_trail,
                    'language': 'en',
                    'country': 'GB',
                    'episodeMaxLimit': 20,
                    'allowSponsoredRecommendations': {
                        'appId': '3.8.2-webapp#e8e565e',
                        'country': 'GB',
                        'language': 'en',
                        'pageType': 'VIEW_TITLE_DETAIL',
                        'placement': 'DETAIL_PAGE',
                        'platform': 'WEB',
                        'supportedObjectTypes': [
                            'MOVIE',
                            'SHOW',
                            'GENERIC_TITLE_LIST',
                            'SHOW_SEASON',
                        ],
                        'supportedFormats': [
                            'IMAGE',
                            'VIDEO',
                        ],
                        'testingMode': False,
                    },
                },
                'query': 'query GetUrlTitleDetails($fullPath: String!, $country: Country!, $language: Language!, $episodeMaxLimit: Int, $platform: Platform! = WEB, $allowSponsoredRecommendations: SponsoredRecommendationsInput, $format: ImageFormat, $backdropProfile: BackdropProfile) {\n  urlV2(fullPath: $fullPath) {\n    id\n    metaDescription\n    metaKeywords\n    metaRobots\n    metaTitle\n    heading1\n    heading2\n    htmlContent\n    node {\n      ...TitleDetails\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TitleDetails on Node {\n  id\n  __typename\n  ... on MovieOrShowOrSeason {\n    plexPlayerOffers: offers(\n      country: $country\n      platform: $platform\n      filter: {packages: ["pxp"]}\n    ) {\n      id\n      standardWebURL\n      package {\n        id\n        packageId\n        clearName\n        technicalName\n        shortName\n        __typename\n      }\n      __typename\n    }\n    maxOfferUpdatedAt(country: $country, platform: WEB)\n    appleOffers: offers(\n      country: $country\n      platform: $platform\n      filter: {packages: ["atp", "itu"]}\n    ) {\n      ...TitleOffer\n      __typename\n    }\n    disneyOffersCount: offerCount(\n      country: $country\n      platform: $platform\n      filter: {packages: ["dnp"]}\n    )\n    objectType\n    objectId\n    offerCount(country: $country, platform: $platform)\n    offers(country: $country, platform: $platform) {\n      monetizationType\n      elementCount\n      package {\n        id\n        packageId\n        clearName\n        __typename\n      }\n      __typename\n    }\n    watchNowOffer(country: $country, platform: $platform) {\n      id\n      standardWebURL\n      __typename\n    }\n    promotedBundles(country: $country, platform: $platform) {\n      promotionUrl\n      __typename\n    }\n    availableTo(country: $country, platform: $platform) {\n      availableCountDown(country: $country)\n      availableToDate\n      package {\n        id\n        shortName\n        __typename\n      }\n      __typename\n    }\n    fallBackClips: content(country: "US", language: "en") {\n      videobusterClips: clips(providers: [VIDEOBUSTER]) {\n        ...TrailerClips\n        __typename\n      }\n      dailymotionClips: clips(providers: [DAILYMOTION]) {\n        ...TrailerClips\n        __typename\n      }\n      __typename\n    }\n    content(country: $country, language: $language) {\n      backdrops {\n        backdropUrl\n        __typename\n      }\n      fullBackdrops: backdrops(profile: S1920, format: JPG) {\n        backdropUrl\n        __typename\n      }\n      clips {\n        ...TrailerClips\n        __typename\n      }\n      videobusterClips: clips(providers: [VIDEOBUSTER]) {\n        ...TrailerClips\n        __typename\n      }\n      dailymotionClips: clips(providers: [DAILYMOTION]) {\n        ...TrailerClips\n        __typename\n      }\n      externalIds {\n        imdbId\n        __typename\n      }\n      fullPath\n      genres {\n        shortName\n        __typename\n      }\n      posterUrl\n      fullPosterUrl: posterUrl(profile: S718, format: JPG)\n      runtime\n      isReleased\n      scoring {\n        imdbScore\n        imdbVotes\n        tmdbPopularity\n        tmdbScore\n        jwRating\n        __typename\n      }\n      shortDescription\n      title\n      originalReleaseYear\n      originalReleaseDate\n      upcomingReleases(releaseTypes: DIGITAL) {\n        releaseCountDown(country: $country)\n        releaseDate\n        label\n        package {\n          id\n          packageId\n          shortName\n          clearName\n          __typename\n        }\n        __typename\n      }\n      ... on MovieOrShowContent {\n        originalTitle\n        ageCertification\n        credits {\n          role\n          name\n          characterName\n          personId\n          __typename\n        }\n        interactions {\n          dislikelistAdditions\n          likelistAdditions\n          votesNumber\n          __typename\n        }\n        productionCountries\n        __typename\n      }\n      ... on SeasonContent {\n        seasonNumber\n        interactions {\n          dislikelistAdditions\n          likelistAdditions\n          votesNumber\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    popularityRank(country: $country) {\n      rank\n      trend\n      trendDifference\n      __typename\n    }\n    __typename\n  }\n  ... on MovieOrShow {\n    watchlistEntryV2 {\n      createdAt\n      __typename\n    }\n    likelistEntry {\n      createdAt\n      __typename\n    }\n    dislikelistEntry {\n      createdAt\n      __typename\n    }\n    customlistEntries {\n      createdAt\n      genericTitleList {\n        id\n        __typename\n      }\n      __typename\n    }\n    similarTitlesV2(\n      country: $country\n      allowSponsoredRecommendations: $allowSponsoredRecommendations\n    ) {\n      sponsoredAd {\n        ...SponsoredAd\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  ... on Movie {\n    permanentAudiences\n    seenlistEntry {\n      createdAt\n      __typename\n    }\n    __typename\n  }\n  ... on Show {\n    permanentAudiences\n    totalSeasonCount\n    seenState(country: $country) {\n      progress\n      seenEpisodeCount\n      __typename\n    }\n    tvShowTrackingEntry {\n      createdAt\n      __typename\n    }\n    seasons(sortDirection: DESC) {\n      id\n      objectId\n      objectType\n      totalEpisodeCount\n      availableTo(country: $country, platform: $platform) {\n        availableToDate\n        availableCountDown(country: $country)\n        package {\n          id\n          shortName\n          __typename\n        }\n        __typename\n      }\n      content(country: $country, language: $language) {\n        posterUrl\n        seasonNumber\n        fullPath\n        title\n        upcomingReleases(releaseTypes: DIGITAL) {\n          releaseDate\n          releaseCountDown(country: $country)\n          package {\n            id\n            shortName\n            __typename\n          }\n          __typename\n        }\n        isReleased\n        originalReleaseYear\n        __typename\n      }\n      show {\n        id\n        objectId\n        objectType\n        watchlistEntryV2 {\n          createdAt\n          __typename\n        }\n        content(country: $country, language: $language) {\n          title\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    recentEpisodes: episodes(\n      sortDirection: DESC\n      limit: 3\n      releasedInCountry: $country\n    ) {\n      id\n      objectId\n      content(country: $country, language: $language) {\n        title\n        shortDescription\n        episodeNumber\n        seasonNumber\n        isReleased\n        upcomingReleases {\n          releaseDate\n          label\n          __typename\n        }\n        __typename\n      }\n      seenlistEntry {\n        createdAt\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  ... on Season {\n    totalEpisodeCount\n    episodes(limit: $episodeMaxLimit) {\n      id\n      objectType\n      objectId\n      seenlistEntry {\n        createdAt\n        __typename\n      }\n      content(country: $country, language: $language) {\n        title\n        shortDescription\n        episodeNumber\n        seasonNumber\n        isReleased\n        upcomingReleases(releaseTypes: DIGITAL) {\n          releaseDate\n          label\n          package {\n            id\n            packageId\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    show {\n      id\n      objectId\n      objectType\n      totalSeasonCount\n      customlistEntries {\n        createdAt\n        genericTitleList {\n          id\n          __typename\n        }\n        __typename\n      }\n      tvShowTrackingEntry {\n        createdAt\n        __typename\n      }\n      fallBackClips: content(country: "US", language: "en") {\n        videobusterClips: clips(providers: [VIDEOBUSTER]) {\n          ...TrailerClips\n          __typename\n        }\n        dailymotionClips: clips(providers: [DAILYMOTION]) {\n          ...TrailerClips\n          __typename\n        }\n        __typename\n      }\n      content(country: $country, language: $language) {\n        title\n        ageCertification\n        fullPath\n        genres {\n          shortName\n          __typename\n        }\n        credits {\n          role\n          name\n          characterName\n          personId\n          __typename\n        }\n        productionCountries\n        externalIds {\n          imdbId\n          __typename\n        }\n        upcomingReleases(releaseTypes: DIGITAL) {\n          releaseDate\n          __typename\n        }\n        backdrops {\n          backdropUrl\n          __typename\n        }\n        posterUrl\n        isReleased\n        videobusterClips: clips(providers: [VIDEOBUSTER]) {\n          ...TrailerClips\n          __typename\n        }\n        dailymotionClips: clips(providers: [DAILYMOTION]) {\n          ...TrailerClips\n          __typename\n        }\n        __typename\n      }\n      seenState(country: $country) {\n        progress\n        __typename\n      }\n      watchlistEntryV2 {\n        createdAt\n        __typename\n      }\n      dislikelistEntry {\n        createdAt\n        __typename\n      }\n      likelistEntry {\n        createdAt\n        __typename\n      }\n      similarTitlesV2(\n        country: $country\n        allowSponsoredRecommendations: $allowSponsoredRecommendations\n      ) {\n        sponsoredAd {\n          ...SponsoredAd\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    seenState(country: $country) {\n      progress\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TitleOffer on Offer {\n  id\n  presentationType\n  monetizationType\n  retailPrice(language: $language)\n  retailPriceValue\n  currency\n  lastChangeRetailPriceValue\n  type\n  package {\n    id\n    packageId\n    clearName\n    technicalName\n    icon(profile: S100)\n    __typename\n  }\n  standardWebURL\n  elementCount\n  availableTo\n  deeplinkRoku: deeplinkURL(platform: ROKU_OS)\n  subtitleLanguages\n  videoTechnology\n  audioTechnology\n  audioLanguages\n  __typename\n}\n\nfragment TrailerClips on Clip {\n  sourceUrl\n  externalId\n  provider\n  name\n  __typename\n}\n\nfragment SponsoredAd on SponsoredRecommendationAd {\n  bidId\n  holdoutGroup\n  campaign {\n    name\n    externalTrackers {\n      type\n      data\n      __typename\n    }\n    hideRatings\n    hideDetailPageButton\n    promotionalImageUrl\n    promotionalVideo {\n      url\n      __typename\n    }\n    promotionalTitle\n    promotionalText\n    promotionalProviderLogo\n    watchNowLabel\n    watchNowOffer {\n      standardWebURL\n      presentationType\n      monetizationType\n      package {\n        id\n        packageId\n        shortName\n        clearName\n        icon\n        __typename\n      }\n      __typename\n    }\n    nodeOverrides {\n      nodeId\n      promotionalImageUrl\n      watchNowOffer {\n        standardWebURL\n        __typename\n      }\n      __typename\n    }\n    node {\n      nodeId: id\n      __typename\n      ... on MovieOrShowOrSeason {\n        content(country: $country, language: $language) {\n          fullPath\n          posterUrl\n          title\n          originalReleaseYear\n          scoring {\n            imdbScore\n            __typename\n          }\n          externalIds {\n            imdbId\n            __typename\n          }\n          backdrops(format: $format, profile: $backdropProfile) {\n            backdropUrl\n            __typename\n          }\n          isReleased\n          __typename\n        }\n        objectId\n        objectType\n        offers(country: $country, platform: $platform) {\n          monetizationType\n          presentationType\n          package {\n            id\n            packageId\n            __typename\n          }\n          id\n          __typename\n        }\n        __typename\n      }\n      ... on MovieOrShow {\n        watchlistEntryV2 {\n          createdAt\n          __typename\n        }\n        __typename\n      }\n      ... on Show {\n        seenState(country: $country) {\n          seenEpisodeCount\n          __typename\n        }\n        __typename\n      }\n      ... on Season {\n        content(country: $country, language: $language) {\n          seasonNumber\n          __typename\n        }\n        show {\n          __typename\n          id\n          content(country: $country, language: $language) {\n            originalTitle\n            __typename\n          }\n          watchlistEntryV2 {\n            createdAt\n            __typename\n          }\n        }\n        __typename\n      }\n      ... on GenericTitleList {\n        followedlistEntry {\n          createdAt\n          name\n          __typename\n        }\n        id\n        type\n        content(country: $country, language: $language) {\n          name\n          visibility\n          __typename\n        }\n        titles(country: $country, first: 40) {\n          totalCount\n          edges {\n            cursor\n            node {\n              content(country: $country, language: $language) {\n                fullPath\n                posterUrl\n                title\n                originalReleaseYear\n                scoring {\n                  imdbScore\n                  __typename\n                }\n                isReleased\n                __typename\n              }\n              id\n              objectId\n              objectType\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n    }\n    __typename\n  }\n  __typename\n}\n',
            }

            response = requests.post('https://apis.justwatch.com/graphql', headers=headers, json=json_data)
            details = response.json()["data"]["urlV2"]
            metadesc = details["metaDescription"]
            fullposterurl_trail = details["node"]["content"]["fullPosterUrl"]
            shortdesc = details["node"]["content"]["shortDescription"]
            agecertification = details["node"]["content"]["ageCertification"]
            release_year = details["node"]["content"]["originalReleaseYear"]
            release_date = details["node"]["content"]["originalReleaseDate"]
            try:
                season = details["node"]["totalSeasonCount"]
            except:
                season = ''
            try:
                episodes = details["node"]["seasons"][0]["totalEpisodeCount"]
            except:
                episodes = ''
            cast_data = []
            for elem in details["node"]["content"]["credits"]:
                cast = {}
                cast["role"] = elem["role"]
                cast["name"] = elem["name"]
                cast["characterName"] = elem["characterName"]
                cast_data.append(cast)
            production_country = ', '.join(details["node"]["content"]["productionCountries"])
            data_dict["Age rating"] = agecertification
            data_dict['Season'] = season
            data_dict['MainLink'] = 'https://www.justwatch.com/in/provider/netflix'
            data_dict['Network'] = 'Netflix'
            data_dict['Synopsis'] = shortdesc
            data_dict['No. of Episodes'] = episodes
            # data_dict['Full Poster URL Trail'] = fullposterurl_trail
            data_dict['What to Know'] = shortdesc
            # data_dict['Release Year'] = release_year
            data_dict['Date Published'] = release_date
            data_dict['Cast'] = cast_data
            data_dict['Person Name - Character Name'] = cast_data
            data_dict["Director"] = ''
            data_dict['Production Country'] = ''
            data_dict['Date Created'] = ''
            data_dict['Country Streaming in'] = ''
            # print(metadesc, fullposterurl_trail, shortdesc, release_year, release_date, production_country, cast_data, sep='\n')
            master_data.append(data_dict)
            # pprint.pprint(data_dict)

df = pd.DataFrame(master_data)
df.to_excel('Justwatch_Netflix_2019-24_200524.xlsx', index=False)

# Director
# Prduction Country --> Production Country Code
# Country Streaming In
# Date Created