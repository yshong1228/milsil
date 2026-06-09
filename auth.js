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
    var nameEl=document.getElementById('mypageName');
    var levelEl=document.getElementById('mypageLevel');
    var statsEl=document.getElementById('mypageStatsRow');

    if(avatarEl && user.photoURL){ avatarEl.src=user.photoURL; avatarEl.style.display='block'; }
    if(nameEl) nameEl.textContent=me;

    // app.js getMemberStats 활용 (전역 함수)
    var stats={exp:0,avg:0,played:0,reviewCount:0};
    if(typeof getMemberStats==='function') stats=getMemberStats(me)||stats;

    // 레벨 계산 (app.js getLevel 활용)
    var lv=typeof getLevel==='function'?getLevel(stats.exp):Math.floor(stats.exp/5)||1;
    if(levelEl) levelEl.textContent='Lv.'+lv+' 탐정 · EXP '+stats.exp;
    if(statsEl) statsEl.innerHTML=
      '<div class="mypg-stat"><div class="mypg-stat-val">'+stats.played+'</div><div class="mypg-stat-label">총 플레이</div></div>'+
      '<div class="mypg-stat"><div class="mypg-stat-val">'+(stats.avg?stats.avg.toFixed(1):'-')+'</div><div class="mypg-stat-label">평균 점수</div></div>'+
      '<div class="mypg-stat"><div class="mypg-stat-val">'+stats.reviewCount+'</div><div class="mypg-stat-label">작성 리뷰</div></div>'+
      '<div class="mypg-stat"><div class="mypg-stat-val">'+stats.exp+'</div><div class="mypg-stat-label">경험치</div></div>';
  }

  function renderMypageEvents(me){
    var el=document.getElementById('mypageEvents');
    if(!el) return;
    var allEvents=window.events||[];
    var mine=allEvents.filter(function(ev){
      return (ev.participants||[]).some(function(p){ return p.name===me; });
    }).sort(function(a,b){ return (b.date||'').localeCompare(a.date||''); });

    if(!mine.length){ el.innerHTML='<div class="mypage-empty">참가한 파티 기록이 없습니다</div>'; return; }

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

    if(!mine.length){ el.innerHTML='<div class="mypage-empty">게임 점수 기록이 없습니다</div>'; return; }

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
      if(!rvs.length){ el.innerHTML='<div class="mypage-empty">작성한 리뷰가 없습니다</div>'; return; }
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
          updateHeaderProfile(user,data.memberName||null);
          if(linkModal) linkModal.style.display='none';
          checkUnlinkedMembers();
          // 마이페이지 탭 표시
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

  // ── 로그아웃 ──
  window.doSignOut=function(){
    auth.signOut().then(function(){
      if(typeof showToast==='function') showToast('로그아웃 되었습니다');
    }).catch(function(err){
      console.error('[Auth] 로그아웃 실패:',err);
    });
  };

})();
