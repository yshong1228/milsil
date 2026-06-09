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

  try{
    auth=firebase.auth();
    db=firebase.firestore();
  }catch(e){
    console.error('[Auth] 초기화 실패:', e);
    var ov2=document.getElementById('loginOverlay');
    if(ov2) ov2.style.display='none';
    return;
  }

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
      if(grid) grid.innerHTML='<div style="text-align:center;padding:1rem;color:#8b1a1a;font-size:12px">목록 로딩 실패: '+e.message+'</div>';
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
          updateHeaderProfile(user,data.memberName||null);
          if(linkModal) linkModal.style.display='none';
          checkUnlinkedMembers();
          loadNicknameMap();
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
    }
  });

  // ── Google 로그인 ──
  window.signInWithGoogle=function(){
    var provider=new firebase.auth.GoogleAuthProvider();
    provider.setCustomParameters({hl:'ko'});
    auth.signInWithPopup(provider).catch(function(err){
      console.error('[Auth] Google 로그인 실패:',err);
      var msg;
      switch(err.code){
        case 'auth/popup-closed-by-user': msg='로그인이 취소되었습니다';break;
        case 'auth/popup-blocked': msg='팝업이 차단되었습니다. 팝업 허용 후 다시 시도해주세요';break;
        case 'auth/network-request-failed': msg='네트워크 오류가 발생했습니다';break;
        default: msg='로그인에 실패했습니다';
      }
      if(typeof showToast==='function') showToast(msg);
    });
  };

  // ── 닉네임 맵 — 전 사이트 반영 ──

  function loadNicknameMap(){
    db.collection('users').get().then(function(snap){
      var map={};
      snap.forEach(function(doc){
        var d=doc.data();
        if(d.memberName && d.nickname && d.nickname.trim()){
          map[d.memberName]=d.nickname.trim();
        }
      });
      window.NICKNAME_MAP=map;
      applyNicknameMap();
    }).catch(function(e){
      console.warn('[Auth] 닉네임 맵 로드 실패:',e);
    });
  }

  function applyNicknameMap(){
    var map=window.NICKNAME_MAP;
    if(!map||!Object.keys(map).length) return;

    // 회원명부 — .member-card-btn[data-mname]
    document.querySelectorAll('.member-card-btn[data-mname]').forEach(function(card){
      var mname=card.getAttribute('data-mname');
      var nick=map[mname];
      if(!nick) return;
      card.querySelectorAll('span').forEach(function(sp){
        if(sp.textContent===mname) sp.textContent=nick;
      });
      var av=card.querySelector('.member-avatar');
      if(av && av.textContent===mname.slice(0,1)) av.textContent=nick.slice(0,1);
    });

    // 모험기록부 — .rv-name (리뷰 작성자)
    document.querySelectorAll('.rv-name').forEach(function(el){
      var orig=el.textContent.trim();
      if(map[orig]) el.textContent=map[orig];
    });
  }

  function setupNicknamePatcher(){
    // 닉네임 교체 옵저버 (records-page, members-page)
    ['records-page','members-page'].forEach(function(id){
      var el=document.getElementById(id);
      if(!el) return;
      var timer=null;
      var obs=new MutationObserver(function(mutations){
        var hasNewEl=mutations.some(function(m){
          return m.addedNodes&&Array.prototype.some.call(m.addedNodes,function(n){ return n.nodeType===1; });
        });
        if(!hasNewEl) return;
        clearTimeout(timer);
        timer=setTimeout(applyNicknameMap, 30);
      });
      obs.observe(el,{childList:true,subtree:true});
    });

    // 이름 자동완성 옵저버 (board-page, records-page)
    // records-page에서 리뷰 폼 open 클래스 변경도 감지
    ['board-page','records-page'].forEach(function(id){
      var el=document.getElementById(id);
      if(!el) return;
      var timer=null;
      function scheduleAutoFill(){
        clearTimeout(timer);
        timer=setTimeout(applyAutoFill, 30);
      }
      var obs=new MutationObserver(function(mutations){
        var relevant=mutations.some(function(m){
          if(m.type==='childList'){
            return Array.prototype.some.call(m.addedNodes||[],function(n){ return n.nodeType===1; });
          }
          if(m.type==='attributes' && m.attributeName==='class'){
            return m.target.classList.contains('open');
          }
          return false;
        });
        if(relevant) scheduleAutoFill();
      });
      obs.observe(el,{childList:true,subtree:true,attributes:true,attributeFilter:['class']});
    });
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

  // DOM 준비 후 패처·패치 설치
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded', function(){
      setupNicknamePatcher();
      patchJoinEvent();
      patchSubmitReview();
    });
  }else{
    setupNicknamePatcher();
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
      // 닉네임 맵 즉시 반영
      var me=window.currentLinkedMember;
      if(me){
        if(nickname) window.NICKNAME_MAP[me]=nickname;
        else delete window.NICKNAME_MAP[me];
        applyNicknameMap();
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
