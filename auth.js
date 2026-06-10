// ── 밀실극장 Firebase Auth ──
// app.js 이후 로드됨. firebase.initializeApp() 완료 후 실행.

(function(){
  'use strict';

  if(window.USING_LOCAL_DB || window.IS_FILE_PROTOCOL){
    var ov=document.getElementById('loginOverlay');
    if(ov) ov.style.display='none';
    window.signInWithGoogle=function(){};
    window.doSignOut=function(){};
    return;
  }

  var auth, db;
  window.NICKNAME_MAP={};
  window.AVATAR_MAP={};
  var _profileMapLoaded=false;

  try{
    auth=firebase.auth();
    db=firebase.firestore();
  }catch(e){
    console.error('[Auth] 초기화 실패:', e);
    var ov2=document.getElementById('loginOverlay');
    if(ov2) ov2.style.display='none';
    return;
  }

  // ── 모바일 sessionStorage 파티션 대응 ──
  // Firebase compat SDK는 초기화 시 내부적으로 getRedirectResult()를 호출한다.
  // 모바일 브라우저(iOS Safari ITP, Android WebView)는 OAuth redirect 후
  // sessionStorage 파티션이 달라져 "missing initial state" 에러가 발생한다.
  // 명시적으로 먼저 처리해 unhandled rejection을 막는다.
  auth.getRedirectResult().then(function(result){
    // redirect 로그인 완료 시 onAuthStateChanged가 자동으로 처리
    if(result && result.user) console.log('[Auth] 리다이렉트 로그인 완료');
  }).catch(function(err){
    var isMissingState = err.message && err.message.indexOf('initial state') !== -1;
    if(err.code==='auth/internal-error' ||
       err.code==='auth/web-storage-unsupported' ||
       isMissingState){
      // 모바일 스토리지 파티션 문제 — 팝업 기반 앱에서는 무시해도 안전
      console.warn('[Auth] 리다이렉트 상태 확인 실패 (모바일/파티션):', err.code);
      return;
    }
    console.error('[Auth] getRedirectResult 에러:', err);
  });

  // ── 헤더 프로필 업데이트 ──
  function updateHeaderProfile(user, memberName){
    var profile=document.getElementById('userProfile');
    var avatarImg=document.getElementById('userAvatarImg');
    var nameEl=document.getElementById('userNameText');
    if(profile) profile.style.display='flex';
    if(avatarImg){
      avatarImg.src=user.photoURL||'';
      avatarImg.style.display=user.photoURL?'block':'none';
    }
    if(nameEl){
      var label=memberName||user.displayName||user.email||'';
      nameEl.textContent=label.length>8?label.substring(0,8)+'…':label;
      nameEl.title=label;
    }
  }

  // 미연결 배너를 볼 수 있는 관리자 목록
  var ADMIN_MEMBERS=['양석','추추','항영','사원'];

  // ── 미연결 회원 배너 ── 관리자에게만 표시
  function checkUnlinkedMembers(){
    if(ADMIN_MEMBERS.indexOf(window.currentLinkedMember)<0) return;
    db.collection('members').get().then(function(snap){
      var unlinked=[];
      snap.forEach(function(doc){
        var d=doc.data();
        if(!d.uid) unlinked.push(d.memberName||doc.id);
      });

      var banner=document.getElementById('unlinkedBanner');
      var countEl=document.getElementById('unlinkedBannerCount');
      var namesEl=document.getElementById('unlinkedBannerNames');
      if(!banner) return;

      if(unlinked.length>0){
        if(countEl) countEl.textContent=unlinked.length;
        if(namesEl) namesEl.textContent=unlinked.join(', ');
        banner.style.display='block';
      }else{
        banner.style.display='none';
      }
    }).catch(function(e){
      console.error('[Auth] 미연결 확인 실패:',e);
    });
  }

  window.dismissUnlinkedBanner=function(){
    var banner=document.getElementById('unlinkedBanner');
    if(banner) banner.style.display='none';
  };

  // ── 본인 확인 모달 ── (Firestore 단일 소스)
  function showLinkModal(user){
    var modal=document.getElementById('linkModal');
    if(!modal) return;

    var nameEl=document.getElementById('linkGoogleName');
    var avatarEl=document.getElementById('linkGoogleAvatar');
    if(nameEl) nameEl.textContent=user.displayName||user.email||'';
    if(avatarEl && user.photoURL){
      avatarEl.src=user.photoURL;
      avatarEl.style.display='block';
    }

    var grid=document.getElementById('linkMemberGrid');
    if(grid) grid.innerHTML='<div style="text-align:center;padding:1rem;color:#a09070;font-family:Cinzel,serif;font-size:12px;letter-spacing:1px">☽ 명부 불러오는 중... ☾</div>';

    var confirmBtn=document.getElementById('linkConfirmBtn');
    if(confirmBtn){ confirmBtn.disabled=true; confirmBtn.textContent='이름을 선택해주세요'; }

    modal.style.display='flex';

    db.collection('members').get().then(function(snap){
      if(!grid) return;

      if(snap.empty){
        grid.innerHTML='<div style="text-align:center;padding:1.5rem;color:#a09070;font-size:12px">등록된 회원이 없습니다<br>관리자에게 문의해주세요</div>';
        return;
      }

      var members=[];
      snap.forEach(function(doc){
        var d=doc.data();
        members.push({ name: d.memberName||doc.id, taken: !!(d.uid) });
      });
      members.sort(function(a,b){ return (a.name).localeCompare(b.name,'ko'); });

      grid.innerHTML='';
      members.forEach(function(m){
        var btn=document.createElement('button');
        btn.className='link-member-btn'+(m.taken?' link-member-taken':'');
        btn.setAttribute('data-name',m.name);
        btn.disabled=m.taken;
        btn.innerHTML='<span class="link-member-name">'+m.name+'</span>'
          +(m.taken?'<span class="link-taken-badge">연결됨</span>':'');

        if(!m.taken){
          btn.onclick=function(){
            document.querySelectorAll('.link-member-btn').forEach(function(b){ b.classList.remove('selected'); });
            btn.classList.add('selected');
            if(confirmBtn){
              confirmBtn.disabled=false;
              confirmBtn.textContent='✦  '+m.name+'으로 연결하기';
            }
          };
        }
        grid.appendChild(btn);
      });
    }).catch(function(e){
      console.error('[Auth] 회원 목록 로딩 실패:',e);
      if(grid) grid.innerHTML='<div style="text-align:center;padding:1rem;color:#8b1a1a;font-size:12px">목록 로딩 실패: '+esc(e.message)+'</div>';
    });
  }

  // ── 연결 확정 ──
  window.confirmMemberLink=function(){
    var selected=document.querySelector('.link-member-btn.selected');
    if(!selected) return;
    var memberName=selected.getAttribute('data-name');
    var user=auth.currentUser;
    if(!user||!memberName) return;

    var confirmBtn=document.getElementById('linkConfirmBtn');
    if(confirmBtn){ confirmBtn.disabled=true; confirmBtn.textContent='연결 중...'; }

    db.collection('members').doc(memberName).get().then(function(doc){
      if(doc.exists && doc.data().uid){
        if(confirmBtn){ confirmBtn.disabled=false; confirmBtn.textContent='✦  '+memberName+'으로 연결하기'; }
        if(typeof showToast==='function') showToast('이미 다른 계정이 연결된 이름입니다');
        selected.disabled=true;
        selected.classList.remove('selected');
        selected.classList.add('link-member-taken');
        selected.innerHTML='<span class="link-member-name">'+memberName+'</span><span class="link-taken-badge">연결됨</span>';
        if(confirmBtn) confirmBtn.textContent='이름을 선택해주세요';
        return Promise.reject('taken');
      }

      var batch=db.batch();
      batch.set(db.collection('users').doc(user.uid),{
        memberName:memberName,
        email:user.email||'',
        displayName:user.displayName||'',
        photoURL:user.photoURL||'',
        linkedAt:firebase.firestore.FieldValue.serverTimestamp()
      });
      batch.set(db.collection('members').doc(memberName),{
        memberName:memberName,
        uid:user.uid,
        email:user.email||''
      },{merge:true});
      return batch.commit();
    }).then(function(){
      var modal=document.getElementById('linkModal');
      if(modal) modal.style.display='none';
      window.currentLinkedMember=memberName;
      updateHeaderProfile(user,memberName);
      if(typeof showToast==='function') showToast('✦  '+memberName+'님 연결 완료');
      checkUnlinkedMembers();
    }).catch(function(e){
      if(e==='taken') return;
      console.error('[Auth] 연결 실패:',e);
      if(confirmBtn){ confirmBtn.disabled=false; confirmBtn.textContent='✦  '+memberName+'으로 연결하기'; }
      if(typeof showToast==='function') showToast('연결 실패: '+e.message);
    });
  };

  // ── 유틸 ──
  function esc(str){
    return String(str||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }
  function scoreClass(s){
    return s>=8?'s-great':s>=6.5?'s-good':s>=5?'s-mid':s>=3?'s-low':'s-bad';
  }

  // ── switchTab 오버라이드 — 마이페이지 숨김 보장 ──
  var _origSwitchTab=null;
  function patchSwitchTab(){
    if(typeof window.switchTab!=='function') return false;
    _origSwitchTab=window.switchTab;
    window.switchTab=function(tab){
      var mp=document.getElementById('mypage-page');
      if(mp) mp.style.display='none';
      var btn=document.getElementById('tab-mypage');
      if(btn) btn.classList.remove('active');
      _origSwitchTab(tab);
    };
    return true;
  }
  // app.js가 로드된 직후 auth.js가 실행되므로 즉시 패치 가능
  if(!patchSwitchTab()){
    // 혹시 늦게 정의되는 경우 대비
    setTimeout(patchSwitchTab, 500);
  }

  // ── 마이페이지 전환 ──
  window.switchToMyPage=function(){
    // 다른 탭 비활성화
    ['board','records','members','findgame'].forEach(function(t){
      var btn=document.getElementById('tab-'+t);
      if(btn) btn.className='tab-btn';
    });
    var bp=document.getElementById('board-page'); if(bp) bp.classList.add('hidden');
    ['records-page','members-page','findgame-page'].forEach(function(id){
      var el=document.getElementById(id); if(el) el.classList.remove('active');
    });
    var btn=document.getElementById('tab-mypage');
    if(btn) btn.className='tab-btn active';
    var mp=document.getElementById('mypage-page');
    if(mp) mp.style.display='block';
    renderMyPage();
  };

  // ── 마이페이지 렌더링 ──
  function renderMyPage(){
    var me=window.currentLinkedMember;
    var user=window.currentUser;
    if(!me||!user) return;
    renderMypageProfile(me, user);
    renderMypageEvents(me);
    renderMypageGames(me);
    renderMypageReviews(me);
  }

  function renderMypageProfile(me, user){
    var avatarEl=document.getElementById('mypageAvatar');
    var emojiEl=document.getElementById('mypageAvatarEmoji');
    var nameEl=document.getElementById('mypageName');
    var levelEl=document.getElementById('mypageLevel');
    var statsEl=document.getElementById('mypageStatsRow');
    var numEl=document.getElementById('cfileNumber');

    var profile=window.currentUserProfile||{};
    var avatarType=profile.avatarType||'google';
    var avatarEmoji=profile.avatarEmoji||'🔍';
    var displayName=profile.nickname||me;

    // 아바타 표시
    if(avatarType==='emoji'){
      if(avatarEl) avatarEl.style.display='none';
      if(emojiEl){ emojiEl.textContent=avatarEmoji; emojiEl.style.display='flex'; }
    }else{
      if(emojiEl) emojiEl.style.display='none';
      if(avatarEl && user.photoURL){ avatarEl.src=user.photoURL; avatarEl.style.display='block'; }
    }
    if(nameEl) nameEl.textContent=displayName;

    // 케이스파일 번호 — 이름 문자 코드 기반 4자리
    if(numEl){
      var hash=0;
      for(var i=0;i<me.length;i++) hash=(hash*31+me.charCodeAt(i))&0xffff;
      numEl.textContent='#'+String(hash).padStart(4,'0').slice(-4);
    }

    var stats={exp:0,avg:0,played:0,reviewCount:0};
    if(typeof getMemberStats==='function') stats=getMemberStats(me)||stats;

    var lv=typeof getLevel==='function'?getLevel(stats.exp):Math.floor(stats.exp/5)||1;
    if(levelEl) levelEl.textContent='Lv.'+lv+' 탐정 · EXP '+stats.exp;
    if(statsEl) statsEl.innerHTML=
      '<div class="cfile-stat"><div class="cfile-stat-val">'+stats.played+'</div><div class="cfile-stat-label">PLAYS</div></div>'+
      '<div class="cfile-stat"><div class="cfile-stat-val">'+(stats.avg?stats.avg.toFixed(1):'-')+'</div><div class="cfile-stat-label">AVG SCORE</div></div>'+
      '<div class="cfile-stat"><div class="cfile-stat-val">'+stats.reviewCount+'</div><div class="cfile-stat-label">REVIEWS</div></div>'+
      '<div class="cfile-stat"><div class="cfile-stat-val">'+stats.exp+'</div><div class="cfile-stat-label">EXP</div></div>';
  }

  function renderMypageEvents(me){
    var el=document.getElementById('mypageEvents');
    if(!el) return;
    var allEvents=window.events||[];
    var mine=allEvents.filter(function(ev){
      return (ev.participants||[]).some(function(p){ return p.name===me; });
    }).sort(function(a,b){ return (b.date||'').localeCompare(a.date||''); });

    var cntEl=document.getElementById('csec-events-cnt');
    if(!mine.length){
      el.innerHTML='<div class="mypage-empty">참가한 파티 기록이 없습니다</div>';
      if(cntEl) cntEl.textContent='0';
      return;
    }
    if(cntEl) cntEl.textContent=mine.length+'건';

    el.innerHTML=mine.map(function(ev){
      var dateStr=(ev.date||'').replace(/-/g,'.');
      var dow='';
      if(ev.date){ try{ dow=['일','월','화','수','목','금','토'][new Date(ev.date).getDay()]; }catch(e){} }
      var others=(ev.participants||[]).filter(function(p){ return p.name!==me; }).map(function(p){ return esc(p.name); });
      return '<div class="mypg-event-card">'+
        '<div class="mypg-ev-date">'+dateStr+(dow?' ('+dow+')':'')+' '+(ev.time||'')+'</div>'+
        '<div class="mypg-ev-title">'+esc(ev.title||'')+'</div>'+
        (others.length?'<div class="mypg-ev-members">함께한 회원: '+others.join(', ')+'</div>':'')+
        (ev.note?'<div class="mypg-ev-note">'+esc(ev.note)+'</div>':'')+
        '</div>';
    }).join('');
  }

  function renderMypageGames(me){
    var el=document.getElementById('mypageGames');
    if(!el) return;
    var allGames=typeof getMergedGames==='function'?getMergedGames():(window.GAMES||[]);
    var mine=allGames.filter(function(g){ return g.scores&&g.scores[me]!==undefined; })
      .sort(function(a,b){ return (b.scores[me]||0)-(a.scores[me]||0); });

    var cntElG=document.getElementById('csec-games-cnt');
    if(!mine.length){
      el.innerHTML='<div class="mypage-empty">게임 점수 기록이 없습니다</div>';
      if(cntElG) cntElG.textContent='0';
      return;
    }
    if(cntElG) cntElG.textContent=mine.length+'건';

    el.innerHTML=mine.map(function(g){
      var sc=g.scores[me];
      var others=Object.keys(g.scores||{}).filter(function(n){ return n!==me; });
      return '<div class="mypg-game-row">'+
        '<div class="score-ring '+scoreClass(sc)+'" style="width:36px;height:36px;font-size:12px">'+sc+'</div>'+
        '<div class="mypg-game-info">'+
          '<div class="mypg-game-name">'+esc(g.name)+'</div>'+
          '<div class="mypg-game-meta">'+
            (g.diff?'<span>'+esc(g.diff)+'</span>':'')+
            (g.players?'<span>'+esc(g.players)+'</span>':'')+
            (others.length?'<span>'+others.map(esc).join(', ')+'</span>':'')+
          '</div>'+
        '</div>'+
      '</div>';
    }).join('');
  }

  function renderMypageReviews(me){
    var el=document.getElementById('mypageReviews');
    if(!el) return;
    el.innerHTML='<div class="mypg-loading">☽ 리뷰 불러오는 중... ☾</div>';

    db.collection('reviews').where('memberName','==',me).get().then(function(snap){
      var rvs=[];
      snap.forEach(function(doc){ rvs.push(doc.data()); });
      // 정적 LEGACY_REVIEWS 병합 (Firestore에 없는 것만)
      var seen={};
      rvs.forEach(function(r){ seen[(r.gameName||'')+'||'+(r.review||'')]=true; });
      (window.LEGACY_REVIEWS||[]).forEach(function(r){
        if(r.memberName!==me) return;
        var k=(r.gameName||'')+'||'+(r.review||'');
        if(!seen[k]){ seen[k]=true; rvs.push(r); }
      });
      rvs.sort(function(a,b){ return (b.gameName||'').localeCompare(a.gameName||'','ko'); });
      var cntElR=document.getElementById('csec-reviews-cnt');
      if(!rvs.length){
        el.innerHTML='<div class="mypage-empty">작성한 리뷰가 없습니다</div>';
        if(cntElR) cntElR.textContent='0';
        return;
      }
      if(cntElR) cntElR.textContent=rvs.length+'건';
      el.innerHTML=rvs.map(function(rv){
        var sc=rv.score||0;
        return '<div class="mypg-review-card">'+
          '<div class="mypg-rv-header">'+
            '<div class="score-ring '+scoreClass(sc)+'" style="width:34px;height:34px;font-size:11px">'+sc+'</div>'+
            '<div class="mypg-rv-game">'+esc(rv.gameName||'')+'</div>'+
            (rv.isSpoiler?'<span class="mypg-rv-spoiler">스포일러</span>':'')+
          '</div>'+
          (rv.review?'<div class="mypg-rv-text">'+esc(rv.review)+'</div>':'')+
          '</div>';
      }).join('');
    }).catch(function(){
      // Firestore 실패 시 정적 데이터만
      var rvs=(window.LEGACY_REVIEWS||[]).filter(function(r){ return r.memberName===me; });
      if(!rvs.length){ el.innerHTML='<div class="mypage-empty">작성한 리뷰가 없습니다</div>'; return; }
      el.innerHTML=rvs.map(function(rv){
        var sc=rv.score||0;
        return '<div class="mypg-review-card">'+
          '<div class="mypg-rv-header">'+
            '<div class="score-ring '+scoreClass(sc)+'" style="width:34px;height:34px;font-size:11px">'+sc+'</div>'+
            '<div class="mypg-rv-game">'+esc(rv.gameName||'')+'</div>'+
          '</div>'+
          (rv.review?'<div class="mypg-rv-text">'+esc(rv.review)+'</div>':'')+
          '</div>';
      }).join('');
    });
  }

  // ── Auth 상태 변경 감지 ──
  auth.onAuthStateChanged(function(user){
    window.currentUser=user;
    var overlay=document.getElementById('loginOverlay');
    var profile=document.getElementById('userProfile');
    var linkModal=document.getElementById('linkModal');

    if(user){
      if(overlay) overlay.style.display='none';

      db.collection('users').doc(user.uid).get().then(function(doc){
        if(doc.exists){
          var data=doc.data();
          window.currentLinkedMember=data.memberName||null;
          window.currentUserProfile={
            nickname: data.nickname||'',
            realName: data.realName||'',
            avatarType: data.avatarType||'google',
            avatarEmoji: data.avatarEmoji||'🔍'
          };
          // 구글 photoURL을 Firestore에 최신화 (비동기, 실패 무시)
          if(user.photoURL) db.collection('users').doc(user.uid).set({photoURL:user.photoURL},{merge:true}).catch(function(){});
          updateHeaderProfile(user,data.memberName||null);
          if(linkModal) linkModal.style.display='none';
          checkUnlinkedMembers();
          loadMemberProfileMap();
          applyAutoFill();
          var mpBtn=document.getElementById('tab-mypage');
          if(mpBtn) mpBtn.style.display='flex';
        }else{
          if(profile) profile.style.display='none';
          showLinkModal(user);
        }
      }).catch(function(e){
        console.error('[Auth] users 조회 실패:',e);
        updateHeaderProfile(user,null);
      });
    }else{
      if(overlay) overlay.style.display='flex';
      if(profile) profile.style.display='none';
      if(linkModal) linkModal.style.display='none';
      var banner=document.getElementById('unlinkedBanner');
      if(banner) banner.style.display='none';
      window.currentLinkedMember=null;
      _profileMapLoaded=false;
    }
  });

  // ── 환경 감지 (화이트리스트 방식) ──
  // 블랙리스트(앱 이름 나열)는 대응 못 하는 앱이 항상 생긴다.
  // 반대로 "Google OAuth가 실제 동작하는 브라우저"만 화이트리스트로 허용.
  var _ua=navigator.userAgent;
  var _isAndroid=/Android/i.test(_ua);
  var _isIOS=/iPhone|iPad|iPod/i.test(_ua);
  var _isMobilePlatform=_isAndroid||_isIOS;

  // Google OAuth가 작동하는 모바일 브라우저 화이트리스트
  var _isSafeBrowser=(function(){
    if(!_isMobilePlatform) return true; // 데스크탑은 모두 허용
    var isChromeMobile =  /Chrome\/\d/.test(_ua) && !/; wv\)/.test(_ua);
    var isSafariiOS    =  _isIOS && /Safari\/\d/.test(_ua) && /Version\/\d/.test(_ua);
    var isSamsungInt   =  /SamsungBrowser\/\d/.test(_ua);
    var isFirefoxMob   =  /Firefox\/\d/.test(_ua);
    var isEdgeMob      =  /EdgA\/\d/.test(_ua);
    var isNaverApp     =  /NAVER\(inapp/.test(_ua);
    var isKakaoLine    =  /KAKAOTALK|Line\/\d|Instagram|FBAN|FBAV/i.test(_ua);
    if(isNaverApp||isKakaoLine) return false;
    return isChromeMobile||isSafariiOS||isSamsungInt||isFirefoxMob||isEdgeMob;
  })();

  // 모바일이면서 안전한 브라우저가 아닌 경우 = WebView / 인앱브라우저
  var _isWebView=_isMobilePlatform&&!_isSafeBrowser;

  // ── 인앱브라우저 안내 모달 ──
  var _siteUrl='https://yshong1228.github.io/milsil/';

  function _showBrowserGuide(){
    var existing=document.getElementById('wvGuide');
    if(existing){ existing.style.display='flex'; return; }

    // Android: intent URL (Chrome 직접 실행, fallback은 Play Store)
    var intentUrl='intent://yshong1228.github.io/milsil/'
      +'#Intent;scheme=https;package=com.android.chrome;'
      +'S.browser_fallback_url=https%3A%2F%2Fplay.google.com%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.android.chrome;end';

    var openBtnHtml=_isAndroid
      ? '<a href="'+intentUrl+'" id="wvChromBtn" style="display:block;background:#b8924a;color:#16100f;padding:13px;border-radius:10px;text-decoration:none;font-size:15px;font-weight:700;margin-bottom:10px;letter-spacing:.3px">Chrome으로 열기</a>'
      : '<div style="background:#1a1210;border:1px solid rgba(217,184,120,.25);border-radius:10px;padding:12px;margin-bottom:12px;word-break:break-all;font-size:12px;color:#b8ae98;user-select:all">'+_siteUrl+'</div>';

    var d=document.createElement('div');
    d.id='wvGuide';
    d.style.cssText='position:fixed;inset:0;z-index:10001;background:rgba(0,0,0,.92);display:flex;align-items:center;justify-content:center;padding:24px;';
    d.innerHTML=
      '<div style="background:#1e1614;border:1px solid rgba(217,184,120,.3);border-radius:18px;padding:30px 22px;max-width:310px;width:100%;text-align:center;font-family:\'Noto Sans KR\',sans-serif">'+
        '<div style="font-size:40px;margin-bottom:16px">🌐</div>'+
        '<div style="font-size:17px;font-weight:700;color:#ece4d4;margin-bottom:10px">'+
          ((_isAndroid?'Chrome':'Safari')+' 에서 열어주세요')+
        '</div>'+
        '<div style="font-size:13px;line-height:1.75;color:#b8ae98;margin-bottom:22px">'+
          '현재 앱 내 브라우저에서는<br>Google 로그인을 사용할 수 없습니다.<br>'+
          (_isAndroid?'Chrome':'Safari')+' 에서 접속해 주시면<br>정상적으로 로그인됩니다.'+
        '</div>'+
        openBtnHtml+
        '<button id="wvCopyBtn" style="display:block;width:100%;background:none;border:1px solid rgba(217,184,120,.28);color:#b8ae98;padding:12px;border-radius:10px;cursor:pointer;font-size:13px;margin-bottom:10px;">'+
          '주소 복사하기</button>'+
        '<button onclick="document.getElementById(\'wvGuide\').style.display=\'none\'" '+
          'style="background:none;border:none;color:#5a5040;font-size:12px;cursor:pointer;padding:6px;">닫기</button>'+
      '</div>';
    document.body.appendChild(d);

    // 복사 버튼
    document.getElementById('wvCopyBtn').addEventListener('click',function(){
      if(navigator.clipboard){
        navigator.clipboard.writeText(_siteUrl).then(function(){
          document.getElementById('wvCopyBtn').textContent='복사됨 ✓';
        });
      }else{
        // clipboard API 없는 구형 WebView 대응
        var ta=document.createElement('textarea');
        ta.value=_siteUrl; ta.style.cssText='position:fixed;opacity:0';
        document.body.appendChild(ta);
        try{ ta.select(); document.execCommand('copy'); document.getElementById('wvCopyBtn').textContent='복사됨 ✓'; }
        finally{ document.body.removeChild(ta); }
      }
    });
  }

  // 인앱브라우저로 접속했다면 페이지 로드 시점에 즉시 안내
  // (로그인 버튼을 누르기 전에 미리 알려줌)
  if(_isWebView){
    if(document.readyState==='loading'){
      document.addEventListener('DOMContentLoaded',_showBrowserGuide);
    }else{
      _showBrowserGuide();
    }
  }

  // ── Google 로그인 ──
  window.signInWithGoogle=function(){
    if(_isWebView){ _showBrowserGuide(); return; }

    var provider=new firebase.auth.GoogleAuthProvider();
    provider.setCustomParameters({hl:'ko'});

    auth.signInWithPopup(provider).catch(function(err){
      console.error('[Auth] Google 로그인 실패:',err);
      var msg;
      switch(err.code){
        case 'auth/popup-closed-by-user': msg='로그인이 취소되었습니다';break;
        case 'auth/popup-blocked': auth.signInWithRedirect(provider); return;
        case 'auth/network-request-failed': msg='네트워크 오류가 발생했습니다';break;
        default: msg='로그인에 실패했습니다';
      }
      if(typeof showToast==='function') showToast(msg);
    });
  };

  // ── 프로필 맵 (닉네임 + 아바타) — 전 사이트 반영 ──

  // 헬퍼: 디바운스 타이머 맵
  var _debTimers={};
  function _deb(key,fn,ms){ clearTimeout(_debTimers[key]); _debTimers[key]=setTimeout(fn,ms); }

  // 헬퍼: mutation에 새 element 노드가 있는지
  function _hasNewEl(m){ return m.addedNodes&&Array.prototype.some.call(m.addedNodes,function(n){ return n.nodeType===1; }); }

  // users 컬렉션 1회 쿼리로 닉네임·아바타 맵 동시 구축
  function loadMemberProfileMap(){
    if(_profileMapLoaded) return;
    db.collection('users').get().then(function(snap){
      var nickMap={}, avMap={};
      snap.forEach(function(doc){
        var d=doc.data();
        if(!d.memberName) return;
        if(d.nickname&&d.nickname.trim()) nickMap[d.memberName]=d.nickname.trim();
        avMap[d.memberName]={type:d.avatarType||'google', emoji:d.avatarEmoji||'🔍', url:d.photoURL||''};
      });
      window.NICKNAME_MAP=nickMap;
      window.AVATAR_MAP=avMap;
      _profileMapLoaded=true;
      applyMemberProfiles();
    }).catch(function(e){ console.warn('[Auth] 프로필 맵 로드 실패:',e); });
  }

  // 닉네임 교체 + 아바타 교체를 한 번의 DOM 패스로 처리
  function applyMemberProfiles(){
    var nickMap=window.NICKNAME_MAP||{};
    var avMap=window.AVATAR_MAP||{};

    // 회원명부 카드
    document.querySelectorAll('.member-card-btn[data-mname]').forEach(function(card){
      var mname=card.getAttribute('data-mname');
      var nick=nickMap[mname];
      var avData=avMap[mname];

      // 닉네임 교체 (data-nick-set 가드로 재처리 방지)
      if(nick && !card.getAttribute('data-nick-set')){
        card.querySelectorAll('span').forEach(function(sp){
          if(sp.textContent===mname) sp.textContent=nick;
        });
        card.setAttribute('data-nick-set','1');
      }

      // 아바타 교체 (이미 처리된 카드 건너뜀)
      if(avData && !card.getAttribute('data-av-set')){
        var av=card.querySelector('.member-avatar');
        if(av){
          if(avData.type==='emoji'){
            av.textContent=avData.emoji;
            av.style.fontSize='18px';
          }else if(avData.url){
            av.innerHTML='<img src="'+avData.url+'" loading="lazy" alt="">';
          }else if(nick){
            av.textContent=nick.slice(0,1);
          }
          card.setAttribute('data-av-set','1');
        }
      }else if(nick && !avData){
        // 아바타 데이터 없으면 첫글자만 닉네임 첫글자로
        var av2=card.querySelector('.member-avatar');
        if(av2&&av2.textContent===mname.slice(0,1)) av2.textContent=nick.slice(0,1);
      }
    });

    // 모험기록부 리뷰 작성자 이름
    document.querySelectorAll('.rv-name').forEach(function(el){
      var orig=el.textContent.trim();
      if(nickMap[orig]) el.textContent=nickMap[orig];
    });
  }

  // 옵저버 통합 설치 — 페이지당 1개, 역할별로 분리
  function setupObservers(){
    // members-page: 프로필(닉네임+아바타) 패치
    var membersEl=document.getElementById('members-page');
    if(membersEl){
      new MutationObserver(function(muts){
        if(muts.some(_hasNewEl)) _deb('mem',applyMemberProfiles,30);
      }).observe(membersEl,{childList:true,subtree:true});
    }

    // records-page: 프로필 패치 + 리뷰 폼 열릴 때 자동완성
    var recordsEl=document.getElementById('records-page');
    if(recordsEl){
      new MutationObserver(function(muts){
        var newEl=muts.some(_hasNewEl);
        var formOpen=muts.some(function(m){
          return m.type==='attributes'&&m.attributeName==='class'
            &&m.target.classList&&m.target.classList.contains('review-form')
            &&m.target.classList.contains('open');
        });
        if(newEl) _deb('rec-prof',applyMemberProfiles,30);
        if(newEl||formOpen) _deb('rec-fill',applyAutoFill,30);
      }).observe(recordsEl,{childList:true,subtree:true,attributes:true,attributeFilter:['class']});
    }

    // board-page: 파티 폼 자동완성
    var boardEl=document.getElementById('board-page');
    if(boardEl){
      new MutationObserver(function(muts){
        if(muts.some(_hasNewEl)) _deb('board',applyAutoFill,30);
      }).observe(boardEl,{childList:true,subtree:true});
    }
  }

  // ── 이름 자동완성 ──
  function applyAutoFill(){
    var me=window.currentLinkedMember;
    if(!me) return;

    // 리뷰 폼 이름 필드 (.rf-name)
    document.querySelectorAll('.rf-name').forEach(function(inp){
      if(!inp.value.trim()){
        inp.value=me;
        inp.classList.add('autofilled-name');
        inp.title='로그인된 회원으로 자동 입력됩니다';
      }
    });

    // 파티 참가 이름 필드 (.join-input)
    document.querySelectorAll('.join-input').forEach(function(inp){
      if(!inp.value.trim()){
        inp.value=me;
        inp.classList.add('autofilled-name');
        inp.title='로그인된 회원으로 자동 입력됩니다';
      }
    });
  }

  // ── joinEvent 패치 — 빈 이름 필드 자동 주입 ──
  function patchJoinEvent(){
    if(typeof window.joinEvent!=='function') return;
    var _orig=window.joinEvent;
    window.joinEvent=function(id,gender,e){
      var me=window.currentLinkedMember;
      if(me){
        var inputId=(gender==='male'?'jnm_':'jnf_')+id;
        var nameEl=document.getElementById(inputId);
        if(nameEl && !nameEl.value.trim()) nameEl.value=me;
      }
      return _orig.apply(this,arguments);
    };
  }

  // ── submitReview 패치 — 빈 이름 필드 자동 주입 ──
  function patchSubmitReview(){
    if(typeof window.submitReview!=='function') return;
    var _orig=window.submitReview;
    window.submitReview=function(gameName,rank,e){
      var me=window.currentLinkedMember;
      if(me){
        var nameEl=document.getElementById('rvn_'+rank);
        if(nameEl && !nameEl.value.trim()) nameEl.value=me;
      }
      return _orig.apply(this,arguments);
    };
  }

  // DOM 준비 후 옵저버·패치 설치
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded', function(){
      setupObservers();
      patchJoinEvent();
      patchSubmitReview();
    });
  }else{
    setupObservers();
    patchJoinEvent();
    patchSubmitReview();
  }

  // ── 케이스파일 아코디언 토글 ──
  window.toggleCSection=function(key){
    var sec=document.getElementById('csec-'+key);
    if(!sec) return;
    var isOpen=sec.classList.contains('open');
    sec.classList.toggle('open',!isOpen);
  };

  // ── 프로필 수정 모달 ──
  var _peditAvatarType='google';
  var _peditSelectedEmoji='🔍';

  window.openProfileEdit=function(){
    var modal=document.getElementById('profileEditModal');
    if(!modal) return;
    var profile=window.currentUserProfile||{};
    var user=window.currentUser;

    // 현재 값으로 초기화
    _peditAvatarType=profile.avatarType||'google';
    _peditSelectedEmoji=profile.avatarEmoji||'🔍';
    document.getElementById('peditNickname').value=profile.nickname||'';
    document.getElementById('peditRealName').value=profile.realName||'';

    // 탭 상태 반영
    _updatePeditAvatarTab(_peditAvatarType, user);

    // 이모지 선택 상태 표시
    document.querySelectorAll('.pedit-emoji-btn').forEach(function(btn){
      btn.classList.toggle('selected', btn.getAttribute('data-emoji')===_peditSelectedEmoji);
    });

    modal.style.display='flex';
  };

  window.closeProfileEdit=function(){
    var modal=document.getElementById('profileEditModal');
    if(modal) modal.style.display='none';
  };

  window.switchAvatarTab=function(type){
    _peditAvatarType=type;
    var user=window.currentUser;
    _updatePeditAvatarTab(type, user);
  };

  window.selectEmoji=function(btn){
    _peditSelectedEmoji=btn.getAttribute('data-emoji');
    document.querySelectorAll('.pedit-emoji-btn').forEach(function(b){ b.classList.remove('selected'); });
    btn.classList.add('selected');
    // 미리보기 업데이트
    var preview=document.getElementById('peditAvatarPreview');
    if(preview) preview.textContent=_peditSelectedEmoji;
  };

  function _updatePeditAvatarTab(type, user){
    var tabG=document.getElementById('peditTabGoogle');
    var tabE=document.getElementById('peditTabEmoji');
    var grid=document.getElementById('peditEmojiGrid');
    var preview=document.getElementById('peditAvatarPreview');
    if(tabG) tabG.classList.toggle('active', type==='google');
    if(tabE) tabE.classList.toggle('active', type==='emoji');
    if(grid) grid.style.display=type==='emoji'?'grid':'none';
    if(preview){
      preview.innerHTML='';
      if(type==='google' && user && user.photoURL){
        var img=document.createElement('img');
        img.src=user.photoURL;
        preview.appendChild(img);
      }else if(type==='emoji'){
        preview.textContent=_peditSelectedEmoji;
      }else{
        preview.textContent='👤';
      }
    }
  }

  window.saveProfileEdit=function(){
    var user=window.currentUser;
    if(!user) return;
    var btn=document.querySelector('.pedit-save');
    if(btn) btn.disabled=true;

    var nickname=document.getElementById('peditNickname').value.trim();
    var realName=document.getElementById('peditRealName').value.trim();

    var updateData={
      avatarType: _peditAvatarType,
      avatarEmoji: _peditSelectedEmoji,
      nickname: nickname,
      realName: realName
    };

    db.collection('users').doc(user.uid).update(updateData).then(function(){
      window.currentUserProfile=Object.assign(window.currentUserProfile||{}, updateData);
      // 닉네임·아바타 맵 즉시 반영
      var me=window.currentLinkedMember;
      if(me){
        if(nickname) window.NICKNAME_MAP[me]=nickname;
        else delete window.NICKNAME_MAP[me];
        var cur=window.AVATAR_MAP[me]||{};
        window.AVATAR_MAP[me]={type:_peditAvatarType,emoji:_peditSelectedEmoji,url:cur.url||''};
        // 카드 아바타 재렌더를 위해 data-av-set 초기화
        var card=document.querySelector('.member-card-btn[data-mname="'+me+'"]');
        if(card) card.removeAttribute('data-av-set');
        applyMemberProfiles();
      }
      if(typeof showToast==='function') showToast('프로필이 저장되었습니다');
      if(btn) btn.disabled=false;
      window.closeProfileEdit();
      if(me) renderMypageProfile(me, user);
    },function(e){
      console.error('[Auth] 프로필 저장 실패:',e);
      if(typeof showToast==='function') showToast('저장에 실패했습니다');
      if(btn) btn.disabled=false;
    });
  };

  // ── 로그아웃 ──
  window.doSignOut=function(){
    auth.signOut().then(function(){
      if(typeof showToast==='function') showToast('로그아웃 되었습니다');
    }).catch(function(err){
      console.error('[Auth] 로그아웃 실패:',err);
    });
  };

})();
